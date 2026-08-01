use std::time::Duration;

use cqels_core::stream::TimestampedValue;
use cqels_core::window::{TumblingWindow, Window};
use futures::StreamExt;

#[tokio::main]
async fn main() {
    let values = (0..6)
        .map(|index| TimestampedValue::new(index, index * 3_000))
        .collect::<Vec<_>>();
    let window = TumblingWindow::new(Duration::from_secs(10));
    let stream = Box::pin(futures::stream::iter(values));

    let batches = window.apply(stream).collect::<Vec<_>>().await;
    for (index, batch) in batches.iter().enumerate() {
        println!(
            "window {}: {} values [{}..{}]",
            index + 1,
            batch.size(),
            batch.window_start,
            batch.window_end
        );
    }
}
