//! Telegram-бот для подписки на поток ордербука с биржи Bybit.
//!
//! Основные возможности:
//! - Подписка на поток ордербука по одному тикеру на чат.
//! - Выбор глубины ордербука (1 / 50 / 200 / 1000).
//! - Задание интервала отправки сообщений (в миллисекундах).
//! - Кнопка `STOP` под сообщением для остановки подписки.
//!
//! Бот:
//! - Подключается к публичному WebSocket API Bybit.
//! - Автоматически реконнектится при обрыве соединения.
//! - Поддерживает heartbeat (ping/pong), чтобы соединение не разрывалось.
//! - Форматирует ордербук в удобном виде для чтения в Telegram с HTML‑разметкой.

use std::collections::{HashMap, BTreeMap};
use std::sync::Arc;
use teloxide::prelude::*;
use teloxide::types::{InlineKeyboardButton, InlineKeyboardMarkup};
use tokio::sync::{mpsc, RwLock};
use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};
use serde::{Deserialize, Serialize};
use futures_util::{SinkExt, StreamExt};
use ordered_float::OrderedFloat;

/// Сообщение ордербука, приходящее по WebSocket от Bybit.
///
/// Поля соответствуют формату ответа API `/v5/public/linear` (orderbook).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderbookMessage {
    topic: String,
    #[serde(rename = "type")]
    msg_type: String,
    ts: u64,
    data: OrderbookData,
    cts: u64,
}

/// Данные по ордербуку внутри сообщения.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderbookData {
    /// Торговый инструмент (символ), например `BTCUSDT`.
    s: String,
    /// Массив заявок на покупку: `[price, size]` в виде строк.
    b: Vec<[String; 2]>,
    /// Массив заявок на продажу: `[price, size]` в виде строк.
    a: Vec<[String; 2]>,
    /// Идентификатор обновления (update ID).
    u: u64,
    /// Последовательность (sequence), может отсутствовать.
    seq: Option<u64>,
}

/// Внутреннее состояние ордербука, которое поддерживается в актуальном виде.
///
/// Для удобной сортировки:
/// - `bids` (покупки) хранятся с отрицательной ценой, чтобы `BTreeMap`
///   автоматически выдавал лучшие цены первыми;
/// - `asks` (продажи) хранятся с положительной ценой в естественном порядке.
#[derive(Debug, Clone)]
struct OrderbookState {
    /// Покупки: цена (как отрицательное число) → объём.
    bids: BTreeMap<OrderedFloat<f64>, f64>,
    /// Продажи: цена → объём.
    asks: BTreeMap<OrderedFloat<f64>, f64>,
    /// Последний ID обновления, полученный от Bybit.
    last_update_id: u64,
}

impl OrderbookState {
    /// Создаёт пустое состояние ордербука.
    fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_update_id: 0,
        }
    }

    /// Полностью пересобирает состояние ордербука из снимка (`snapshot`).
    ///
    /// Обычно первый пришедший снимок, после чего применяются дельты (`delta`).
    fn apply_snapshot(&mut self, data: &OrderbookData) {
        self.bids.clear();
        self.asks.clear();
        
        // Заполняем bids
        for bid in &data.b {
            if let (Ok(price), Ok(size)) = (bid[0].parse::<f64>(), bid[1].parse::<f64>()) {
                if size > 0.0 {
                    // Для bids используем отрицательную цену для обратной сортировки
                    self.bids.insert(OrderedFloat(-price), size);
                }
            }
        }
        
        // Заполняем asks
        for ask in &data.a {
            if let (Ok(price), Ok(size)) = (ask[0].parse::<f64>(), ask[1].parse::<f64>()) {
                if size > 0.0 {
                    self.asks.insert(OrderedFloat(price), size);
                }
            }
        }
        
        self.last_update_id = data.u;
    }

    /// Применяет дельту (`delta`) к текущему состоянию ордербука.
    ///
    /// Если объём равен нулю — уровень цены удаляется.
    fn apply_delta(&mut self, data: &OrderbookData) {
        // Обновляем bids
        for bid in &data.b {
            if let (Ok(price), Ok(size)) = (bid[0].parse::<f64>(), bid[1].parse::<f64>()) {
                let neg_price = OrderedFloat(-price);
                if size == 0.0 {
                    self.bids.remove(&neg_price);
                } else {
                    self.bids.insert(neg_price, size);
                }
            }
        }
        
        // Обновляем asks
        for ask in &data.a {
            if let (Ok(price), Ok(size)) = (ask[0].parse::<f64>(), ask[1].parse::<f64>()) {
                let price_key = OrderedFloat(price);
                if size == 0.0 {
                    self.asks.remove(&price_key);
                } else {
                    self.asks.insert(price_key, size);
                }
            }
        }
        
        self.last_update_id = data.u;
    }

    /// Форматирует текущее состояние ордербука в человекочитаемый текст для Telegram.
    ///
    /// - `symbol` — тикер инструмента.
    /// - `depth` — глубина, с которой мы подписались на Bybit.
    /// - `top_n` — сколько лучших уровней цены показать в каждом стакане.
    fn format_orderbook(&self, symbol: &str, depth: u32, top_n: usize) -> String {
        let mut result = format!("📊 <b>Orderbook: {} (глубина {})</b>\n\n", symbol, depth);
        
        // Форматируем лучшие asks (продажи) - сверху
        result.push_str("<b>🔼 ASK (Продажи)</b>\n");
        result.push_str("<code>");
        result.push_str(&format!("{:>14} | {:>14}\n", "Цена", "Объем"));
        result.push_str(&format!("{:->15}+{:->15}\n", "", ""));
        
        for (price, size) in self.asks.iter().take(top_n) {
            result.push_str(&format!("{:>14} | {:>14}\n", 
                format_price(price.into_inner()), 
                format_size(*size)));
        }
        
        result.push_str("</code>\n");
        
        // Разделитель
        result.push_str("\n");
        
        // Форматируем лучшие bids (покупки) - снизу
        result.push_str("<b>🔽 BID (Покупки)</b>\n");
        result.push_str("<code>");
        result.push_str(&format!("{:>14} | {:>14}\n", "Цена", "Объем"));
        result.push_str(&format!("{:->15}+{:->15}\n", "", ""));
        
        for (neg_price, size) in self.bids.iter().take(top_n) {
            let price = -neg_price.into_inner();
            result.push_str(&format!("{:>14} | {:>14}\n", 
                format_price(price), 
                format_size(*size)));
        }
        
        result.push_str("</code>");
        result.push_str(&format!("\n\n<i>Update ID: {}</i>", self.last_update_id));
        
        result
    }
}

/// Форматирование цены в зависимости от её величины
/// (чтобы крупные числа не отображались с избыточной точностью).
fn format_price(price: f64) -> String {
    if price >= 1000.0 {
        format!("{:.2}", price)
    } else if price >= 1.0 {
        format!("{:.4}", price)
    } else {
        format!("{:.8}", price)
    }
}

/// Форматирование объёма аналогично форматированию цены.
fn format_size(size: f64) -> String {
    if size >= 1000.0 {
        format!("{:.2}", size)
    } else if size >= 1.0 {
        format!("{:.4}", size)
    } else {
        format!("{:.8}", size)
    }
}

/// Описание активной подписки для конкретного чата.
#[derive(Debug, Clone)]
struct Subscription {
    /// Символ (тикер), на который подписан пользователь.
    symbol: String,
    /// Интервал отправки ордербука в миллисекундах.
    interval_ms: u32,
    /// Идентификатор Telegram-чата.
    chat_id: ChatId,
    /// Канал для остановки фоновой задачи по запросу пользователя.
    stop_tx: mpsc::Sender<()>,
}

/// Общая структура для хранения подписок:
/// `ChatId` → `Subscription`. Оборачивается в `Arc<RwLock<...>>`
/// для безопасного доступа из нескольких задач.
type SubscriptionMap = Arc<RwLock<HashMap<ChatId, Subscription>>>;

/// Парсинг текстового сообщения пользователя в параметры подписки.
///
/// Ожидаемый формат сообщения:
/// ```text
/// Тикер: BTCUSDT
/// Интервал отправки: 1000
/// Глубина: 50
/// ```
///
/// Возвращает `(тикер, интервал_в_мс, глубина)`, либо `None`, если формат неверен.
fn parse_message(text: &str) -> Option<(String, u32, u32)> {
    let lines: Vec<&str> = text.lines().collect();
    
    if lines.len() < 3 {
        return None;
    }
    
    let ticker_line = lines[0].trim();
    if !ticker_line.starts_with("Тикер:") {
        return None;
    }
    let ticker = ticker_line.strip_prefix("Тикер:").unwrap_or("").trim();
    if ticker.is_empty() {
        return None;
    }
    
    let interval_line = lines[1].trim();
    if !interval_line.starts_with("Интервал отправки:") {
        return None;
    }
    let interval_str = interval_line
        .strip_prefix("Интервал отправки:")
        .unwrap_or("")
        .trim();
    if interval_str.is_empty() {
        return None;
    }
    
    let interval: u32 = interval_str.parse().ok()?;
    
    // Парсим глубину
    let depth_line = lines[2].trim();
    if !depth_line.starts_with("Глубина:") {
        return None;
    }
    let depth_str = depth_line.strip_prefix("Глубина:").unwrap_or("").trim();
    if depth_str.is_empty() {
        return None;
    }
    let depth: u32 = depth_str.parse().ok()?;
    
    // Разрешенные значения глубины
    match depth {
        1 | 50 | 200 | 1000 => Some((ticker.to_string(), interval, depth)),
        _ => None,
    }
}

/// Запускает и поддерживает WebSocket‑подключение к Bybit с автореконнектом.
///
/// - Подписывается на топик ордербука.
/// - Поддерживает heartbeat (ping/pong).
/// - Обновляет `orderbook_state` при получении `snapshot` и `delta`.
/// - По сигналу в `stop_rx` корректно завершает работу.
async fn run_websocket_connection(
    ws_url: String,
    topic: String,
    symbol: String,
    orderbook_state: Arc<RwLock<OrderbookState>>,
    mut stop_rx: mpsc::Receiver<()>,
) -> Result<(), ()> {
    let subscribe_msg = serde_json::json!({
        "op": "subscribe",
        "args": [topic.clone()]
    });
    
    let mut reconnect_delay = 1u64;
    const MAX_RECONNECT_DELAY: u64 = 60;
    
    loop {
        // Проверяем, не нужно ли остановиться перед реконнектом
        if stop_rx.try_recv().is_ok() {
            return Err(());
        }
        
        log::info!("Подключение к WebSocket для {}...", symbol);
        
        match connect_async(&ws_url).await {
            Ok((ws_stream, _)) => {
                log::info!("WebSocket подключен для {}", symbol);
                reconnect_delay = 1; // Сбрасываем задержку при успешном подключении
                
                let (write, mut read) = ws_stream.split();
                let write = Arc::new(tokio::sync::Mutex::new(write));
                
                // Отправляем подписку
                {
                    let mut write_guard = write.lock().await;
                        if let Err(e) = write_guard.send(WsMessage::Text(subscribe_msg.to_string())).await {
                        log::error!("Ошибка отправки подписки для {}: {}", symbol, e);
                        let sleep_duration = tokio::time::Duration::from_secs(reconnect_delay);
                        let start = tokio::time::Instant::now();
                        
                        while start.elapsed() < sleep_duration {
                            if stop_rx.try_recv().is_ok() {
                                return Err(()); // Получен сигнал остановки
                            }
                            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                        }
                        reconnect_delay = (reconnect_delay * 2).min(MAX_RECONNECT_DELAY);
                        continue;
                    }
                }
                
                // Запускаем heartbeat (ping каждые 20 секунд)
                let write_ping = write.clone();
                let symbol_ping = symbol.clone();
                let (ping_abort_tx, mut ping_abort_rx) = mpsc::channel::<()>(1);
                
                let ping_task = tokio::spawn(async move {
                    let mut ping_interval = tokio::time::interval(tokio::time::Duration::from_secs(20));
                    ping_interval.tick().await; // Пропускаем первый тик
                    
                    let ping_msg_str = r#"{"op":"ping"}"#;
                    
                    loop {
                        tokio::select! {
                            _ = ping_interval.tick() => {
                                let mut write_guard = write_ping.lock().await;
                                if let Err(e) = write_guard.send(WsMessage::Text(ping_msg_str.to_string())).await {
                                    log::error!("Ошибка отправки ping для {}: {}", symbol_ping, e);
                                    break;
                                }
                            }
                            _ = ping_abort_rx.recv() => {
                                break;
                            }
                        }
                    }
                });
                
                // Читаем сообщения
                let mut connection_lost = false;
                loop {
                    tokio::select! {
                        msg = read.next() => {
                            match msg {
                                Some(Ok(WsMessage::Text(text))) => {
                                    // Проверяем pong ответ
                                    if let Ok(response) = serde_json::from_str::<serde_json::Value>(&text) {
                                        if let Some(op) = response.get("op").and_then(|v| v.as_str()) {
                                            if op == "pong" {
                                                log::debug!("Получен pong для {}", symbol);
                                                continue;
                                            }
                                        }
                                        
                                        // Проверяем подтверждение подписки
                                        if let Some(success) = response.get("success").and_then(|v| v.as_bool()) {
                                            if success {
                                                if let Some(ret_msg) = response.get("ret_msg").and_then(|v| v.as_str()) {
                                                    if ret_msg == "pong" {
                                                        continue; // Это pong ответ
                                                    }
                                                }
                                                log::info!("Подписка подтверждена для {}: {:?}", symbol, response);
                                                continue;
                                            } else {
                                                log::error!("Ошибка подписки для {}: {:?}", symbol, response);
                                                connection_lost = true;
                                                break;
                                            }
                                        }
                                    }
                                    
                                    // Парсим сообщение orderbook
                                    if let Ok(orderbook_msg) = serde_json::from_str::<OrderbookMessage>(&text) {
                                        if orderbook_msg.topic == topic {
                                            let mut state = orderbook_state.write().await;
                                            match orderbook_msg.msg_type.as_str() {
                                                "snapshot" => {
                                                    log::debug!("Получен snapshot для {}", symbol);
                                                    state.apply_snapshot(&orderbook_msg.data);
                                                }
                                                "delta" => {
                                                    state.apply_delta(&orderbook_msg.data);
                                                }
                                                _ => {
                                                    log::warn!("Неизвестный тип сообщения для {}: {}", symbol, orderbook_msg.msg_type);
                                                }
                                            }
                                        }
                                    }
                                }
                                Some(Ok(WsMessage::Close(_))) => {
                                    log::warn!("WebSocket закрыт для {}", symbol);
                                    connection_lost = true;
                                    break;
                                }
                                Some(Err(e)) => {
                                    log::error!("Ошибка WebSocket для {}: {}", symbol, e);
                                    connection_lost = true;
                                    break;
                                }
                                None => {
                                    log::warn!("WebSocket поток завершен для {}", symbol);
                                    connection_lost = true;
                                    break;
                                }
                                _ => {}
                            }
                        }
                        _ = stop_rx.recv() => {
                            log::info!("Получен сигнал остановки WebSocket для {}", symbol);
                            connection_lost = false; // Не реконнектимся
                            break;
                        }
                    }
                }
                
                // Останавливаем ping задачу
                let _ = ping_abort_tx.send(()).await;
                ping_task.abort();
                
                if !connection_lost {
                    // Получен сигнал остановки — выходим без реконнекта
                    return Err(());
                }
                
                log::warn!("Соединение потеряно для {}, переподключаемся через {} сек...", symbol, reconnect_delay);
                let sleep_duration = tokio::time::Duration::from_secs(reconnect_delay);
                let start = tokio::time::Instant::now();
                
                while start.elapsed() < sleep_duration {
                    if stop_rx.try_recv().is_ok() {
                        return Err(()); // Получен сигнал остановки во время ожидания
                    }
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
                
                reconnect_delay = (reconnect_delay * 2).min(MAX_RECONNECT_DELAY);
            }
            Err(e) => {
                log::error!("Ошибка подключения к WebSocket для {}: {}, переподключаемся через {} сек...", symbol, e, reconnect_delay);
                let sleep_duration = tokio::time::Duration::from_secs(reconnect_delay);
                let start = tokio::time::Instant::now();
                
                while start.elapsed() < sleep_duration {
                    if stop_rx.try_recv().is_ok() {
                        return Err(()); // Получен сигнал остановки во время ожидания
                    }
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
                
                reconnect_delay = (reconnect_delay * 2).min(MAX_RECONNECT_DELAY);
            }
        }
    }
}

/// Запускает полный цикл работы для конкретной подписки:
/// - WebSocket‑подключение к Bybit (`run_websocket_connection`);
/// - периодическая отправка отформатированного ордербука в Telegram;
/// - остановка по сигналу и очистка записи о подписке.
async fn start_orderbook_stream(
    bot: Bot,
    symbol: String,
    interval_ms: u32,
    depth: u32,
    chat_id: ChatId,
    subscriptions: SubscriptionMap,
    mut stop_rx: mpsc::Receiver<()>,
) {
    // Публичный WebSocket эндпоинт для линейных контрактов
    let ws_url = format!("wss://stream.bybit.com/v5/public/linear");
    let topic = format!("orderbook.{}.{}", depth, symbol);
    
    log::info!(
        "Запускаем поток orderbook для {} с глубиной {} и интервалом {}ms",
        symbol,
        depth,
        interval_ms
    );
    
    let orderbook_state = Arc::new(RwLock::new(OrderbookState::new()));
    let orderbook_state_clone = orderbook_state.clone();
    
    // Создаем канал для остановки WebSocket задачи
    let (ws_stop_tx, ws_stop_rx) = mpsc::channel(1);
    let ws_stop_tx_clone = ws_stop_tx.clone();
    
    // Запускаем WebSocket задачу с автореконнектом
    let symbol_ws = symbol.clone();
    tokio::spawn(async move {
        let _ = run_websocket_connection(
            ws_url,
            topic,
            symbol_ws,
            orderbook_state_clone,
            ws_stop_rx,
        ).await;
    });
    
    // Запускаем отправку сообщений с интервалом
    let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(interval_ms as u64));
    interval.tick().await; // Пропускаем первый тик
    
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let state = orderbook_state.read().await;
                if !state.bids.is_empty() || !state.asks.is_empty() {
                    let formatted = state.format_orderbook(&symbol, depth, 10);
                    
                    let keyboard = InlineKeyboardMarkup::new(vec![vec![
                        InlineKeyboardButton::callback("🛑 STOP", format!("stop_{}", chat_id.0))
                    ]]);
                    
                    if let Err(e) = bot.send_message(chat_id, formatted)
                        .reply_markup(keyboard)
                        .parse_mode(teloxide::types::ParseMode::Html)
                        .await {
                        log::error!("Ошибка отправки сообщения: {}", e);
                        break;
                    }
                }
            }
            _ = stop_rx.recv() => {
                log::info!("Получен сигнал остановки для {}", symbol);
                // Отправляем сигнал остановки в WebSocket задачу
                let _ = ws_stop_tx_clone.send(()).await;
                break;
            }
        }
    }
    
    // Удаляем подписку
    subscriptions.write().await.remove(&chat_id);
    log::info!("Подписка остановлена для {}", symbol);
}

#[tokio::main]
async fn main() {
    pretty_env_logger::init();
    log::info!("Запускаем бота...");

    let bot = Bot::from_env();
    let subscriptions: SubscriptionMap = Arc::new(RwLock::new(HashMap::new()));
    
    let handler = dptree::entry()
        .branch(
            Update::filter_callback_query()
                .endpoint(handle_callback_query)
        )
        .branch(
            Update::filter_message()
                .endpoint(handle_message)
        );

    let subscriptions_for_handler = subscriptions.clone();
    
    Dispatcher::builder(bot.clone(), handler)
        .dependencies(dptree::deps![subscriptions_for_handler])
        .enable_ctrlc_handler()
        .build()
        .dispatch()
        .await;
}

async fn handle_callback_query(
    bot: Bot,
    q: CallbackQuery,
    subscriptions: SubscriptionMap,
) -> ResponseResult<()> {
    if let Some(data) = q.data {
        if data.starts_with("stop_") {
            let chat_id_str = data.strip_prefix("stop_").unwrap_or("");
            if let Ok(chat_id_num) = chat_id_str.parse::<i64>() {
                let chat_id = ChatId(chat_id_num);
                
                // Останавливаем подписку
                if let Some(sub) = subscriptions.write().await.remove(&chat_id) {
                    let _ = sub.stop_tx.send(()).await;
                    bot.answer_callback_query(q.id).await?;
                    bot.send_message(chat_id, "🛑 Подписка остановлена").await?;
                }
            }
        }
    }
    Ok(())
}

async fn handle_message(
    bot: Bot,
    msg: Message,
    subscriptions: SubscriptionMap,
) -> ResponseResult<()> {
    if let Some(text) = msg.text() {
        // Проверяем, не является ли это командой /start
        if text == "/start" || text.starts_with("/") {
            return Ok(());
        }
        
        if let Some((ticker, interval, depth)) = parse_message(text) {
            let chat_id = msg.chat.id;
            
            // Останавливаем предыдущую подписку, если есть
            if let Some(old_sub) = subscriptions.write().await.remove(&chat_id) {
                let _ = old_sub.stop_tx.send(()).await;
            }
            
            // Создаем новую подписку
            let (stop_tx, stop_rx) = mpsc::channel(1);
            
            let subscription = Subscription {
                symbol: ticker.clone(),
                interval_ms: interval,
                chat_id,
                stop_tx: stop_tx.clone(),
            };
            
            subscriptions.write().await.insert(chat_id, subscription);
            
            // Запускаем поток orderbook
            tokio::spawn(start_orderbook_stream(
                bot.clone(),
                ticker.clone(),
                interval,
                depth,
                chat_id,
                subscriptions.clone(),
                stop_rx,
            ));
            
            let response = format!(
                "✅ Подписка активирована!\n\nТикер: {}\nИнтервал отправки: {} мс\nГлубина: {}\n\nOrderbook будет отправляться автоматически.",
                ticker, interval, depth
            );
            
            let keyboard = InlineKeyboardMarkup::new(vec![vec![
                InlineKeyboardButton::callback("🛑 STOP", format!("stop_{}", chat_id.0))
            ]]);
            
            bot.send_message(chat_id, response)
                .reply_markup(keyboard)
                .await?;
        } else {
            bot.send_message(msg.chat.id, "ФУУУ").await?;
        }
    }
    Ok(())
}
