use temporalio_macros::TemporalTransfer;

#[derive(TemporalTransfer)]
#[temporal(transfer_type = String)]
struct Model;

impl From<String> for Model {
    fn from(_: String) -> Self {
        Self
    }
}

fn requires_transfer<T: temporalio_common::data_converters::HasTransferType>() {}

fn main() {
    requires_transfer::<Model>();
}
