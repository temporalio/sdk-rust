use temporalio_macros::TemporalTransfer;

#[derive(TemporalTransfer)]
#[temporal(transfer_type = String)]
struct Model;

impl From<&Model> for String {
    fn from(_: &Model) -> Self {
        String::new()
    }
}

impl TryFrom<String> for Model {
    type Error = ();

    fn try_from(_: String) -> Result<Self, Self::Error> {
        Err(())
    }
}

fn main() {}
