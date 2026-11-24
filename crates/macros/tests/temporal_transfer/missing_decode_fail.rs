use temporalio_macros::TemporalTransfer;

#[derive(TemporalTransfer)]
#[temporal(transfer_type = String)]
struct Model;

impl From<&Model> for String {
    fn from(_: &Model) -> Self {
        String::new()
    }
}

fn main() {}
