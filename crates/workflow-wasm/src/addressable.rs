use anyhow::{Context, Result, ensure};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use wasmparser::{DataKind, Encoding, Parser, Payload};

const MAX_BODY_BYTES: u64 = 64 * 1024;
const HASH_DOMAIN: &[u8] = b"temporal-python-cpython-3.14-v1\0";

pub(crate) type Bindings = HashMap<String, Vec<u8>>;

pub(crate) struct ContentStore {
    root: PathBuf,
    published: Bindings,
    bodies: Mutex<HashMap<Vec<u8>, Arc<[u8]>>>,
}

impl ContentStore {
    pub(crate) fn open(root: &Path) -> Result<Self> {
        let root = root.canonicalize().context("opening WASM content store")?;
        let published: HashMap<String, String> = serde_json::from_reader(
            File::open(root.join("bindings.json")).context("opening WASM bindings.json")?,
        )
        .context("reading published WASM bindings")?;
        ensure!(published.len() <= 1024, "too many published WASM slots");
        let published = published
            .into_iter()
            .map(|(slot, hash)| {
                ensure!(
                    !slot.is_empty() && slot.len() <= 128,
                    "invalid WASM slot {slot:?}"
                );
                let digest = hex::decode(hash).context("invalid published WASM digest")?;
                ensure!(
                    digest.len() == 32,
                    "invalid digest length for WASM slot {slot:?}"
                );
                Ok((slot, digest))
            })
            .collect::<Result<_>>()?;
        Ok(Self {
            root,
            published,
            bodies: Mutex::default(),
        })
    }

    fn body(&self, digest: &[u8]) -> Result<Arc<[u8]>> {
        let hex = hex::encode(digest);
        ensure!(digest.len() == 32, "invalid WASM body digest {hex}");
        let mut bodies = self
            .bodies
            .lock()
            .expect("WASM content store mutex poisoned");
        if let Some(source) = bodies.get(digest) {
            return Ok(source.clone());
        }
        let mut bytes = Vec::new();
        File::open(self.root.join(format!("{hex}.wasm")))
            .with_context(|| format!("fetching WASM body {hex}"))?
            .take(MAX_BODY_BYTES + 1)
            .read_to_end(&mut bytes)
            .with_context(|| format!("reading WASM body {hex}"))?;
        ensure!(
            bytes.len() as u64 <= MAX_BODY_BYTES,
            "oversize WASM body {hex}"
        );
        let computed = Sha256::new()
            .chain_update(HASH_DOMAIN)
            .chain_update(&bytes)
            .finalize();
        ensure!(
            computed.as_slice() == digest,
            "digest mismatch for WASM body {hex}"
        );
        let body = capsule_source(&bytes).with_context(|| format!("decoding WASM body {hex}"))?;
        bodies.insert(digest.to_vec(), body.clone());
        Ok(body)
    }
}

fn capsule_source(bytes: &[u8]) -> Result<Arc<[u8]>> {
    // Capsules contain a passive data segment. Decoding it avoids executing a second guest
    // or giving it a separate Python interpreter or workflow context.
    let mut source = None;
    for payload in Parser::new(0).parse_all(bytes) {
        match payload? {
            Payload::Version {
                encoding: Encoding::Module,
                num: 1,
                ..
            }
            | Payload::End(_) => {}
            Payload::DataSection(section) if source.is_none() && section.count() == 1 => {
                let data = section.into_iter().next().expect("checked section count")?;
                ensure!(
                    matches!(data.kind, DataKind::Passive),
                    "expected passive capsule data"
                );
                std::str::from_utf8(data.data).context("capsule source must be UTF-8")?;
                source = Some(Arc::from(data.data));
            }
            _ => anyhow::bail!("unexpected WASM capsule section"),
        }
    }
    source.context("missing capsule source")
}

#[derive(Default)]
pub(crate) struct AddressableState {
    pub(crate) store: Option<Arc<ContentStore>>,
    pub(crate) activation: Option<ActivationBindings>,
}

pub(crate) struct ActivationBindings {
    pub(crate) replaying: bool,
    pub(crate) recorded: Bindings,
    pub(crate) used: Bindings,
}

impl AddressableState {
    pub(crate) fn resolve(&mut self, slot: String) -> Result<(Vec<u8>, Vec<u8>)> {
        let store = self.store.as_ref().context("missing WASM content store")?;
        let activation = self
            .activation
            .as_mut()
            .context("addressable call outside a workflow activation")?;
        let digest = if activation.replaying {
            activation
                .recorded
                .get(&slot)
                .with_context(|| format!("no recorded WASM binding for slot {slot:?}"))?
        } else {
            store
                .published
                .get(&slot)
                .with_context(|| format!("no published WASM binding for slot {slot:?}"))?
        };
        let body = store.body(digest)?;
        if !activation.replaying {
            activation.used.insert(slot, digest.clone());
        }
        Ok((digest.clone(), body.to_vec()))
    }
}
