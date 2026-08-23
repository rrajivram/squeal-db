use std::{collections::HashMap, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::{
    error::StoreError,
    pages::{PageTuple, anytuple::AnyTuplePage, fixedtuple::FixedTuplePage, run::RunPage},
};

// Discriminant persisted per-page (see PageHeader's own field) identifying
// which registered kind a page's raw bytes should be decoded as. A plain
// newtype, not an enum: store can't enumerate every kind a caller might
// register ahead of time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PageContentKind(pub u16);

impl PageContentKind {
    // Reserved so existing on-disk pages (written before this registry
    // existed) keep decoding correctly with no migration: every page
    // written by earlier code implicitly meant one of these two kinds,
    // selected via `record_size.is_some()` — the registry just makes that
    // choice explicit and extensible going forward.
    pub const ANY_TUPLE: PageContentKind = PageContentKind(0);
    pub const FIXED_TUPLE: PageContentKind = PageContentKind(1);
    // Not a legacy on-disk kind like the two above — added alongside
    // crate::run::Run, but pre-registered in builtin() the same way
    // since a Run's pages are first-class store content now, not a
    // per-caller custom registration.
    pub const RUN_TUPLE: PageContentKind = PageContentKind(2);
}

pub type PageContentFactory =
    Arc<dyn Fn(&[u8]) -> Result<Box<dyn PageTuple>, StoreError> + Send + Sync>;

// Per-Db-instance (not a process-global static — see PageBuffer's own
// construction site) map from a page's persisted content kind to the
// factory that can rebuild it from raw bytes. Only needed for
// reconstructing content whose concrete type isn't already known (a
// cache-miss read from disk, or WAL replay); constructing a brand-new page
// of a given kind never needs this, since whoever's allocating it already
// knows the concrete type and can build it directly.
pub struct PageContentRegistry {
    factories: HashMap<PageContentKind, PageContentFactory>,
}

// Factories are opaque closures, not meaningfully printable — this exists
// so PageBuffer (which holds a registry) can keep deriving Debug.
impl std::fmt::Debug for PageContentRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageContentRegistry")
            .field("kinds", &self.factories.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl PageContentRegistry {
    // The two kinds store has always supported, pre-registered so opening a
    // Db with no custom kinds behaves exactly as before.
    pub fn builtin() -> Self {
        let mut registry = Self {
            factories: HashMap::new(),
        };
        registry
            .register(
                PageContentKind::ANY_TUPLE,
                Arc::new(|bytes| Ok(Box::new(AnyTuplePage::from_bytes(bytes)?) as Box<dyn PageTuple>)),
            )
            .expect("built-in kinds register exactly once");
        registry
            .register(
                PageContentKind::FIXED_TUPLE,
                Arc::new(|bytes| Ok(Box::new(FixedTuplePage::from_bytes(bytes)?) as Box<dyn PageTuple>)),
            )
            .expect("built-in kinds register exactly once");
        registry
            .register(
                PageContentKind::RUN_TUPLE,
                Arc::new(|bytes| Ok(Box::new(RunPage::from_bytes(bytes)?) as Box<dyn PageTuple>)),
            )
            .expect("built-in kinds register exactly once");
        registry
    }

    // Errors on a kind that's already registered (built-in or previously
    // custom) rather than silently overwriting it — a silent overwrite
    // would mean every existing on-disk page of that kind decodes through a
    // different factory than the one that wrote it.
    pub fn register(
        &mut self,
        kind: PageContentKind,
        factory: PageContentFactory,
    ) -> Result<(), StoreError> {
        if self.factories.contains_key(&kind) {
            return Err(StoreError::DuplicatePageContentKind(kind.0));
        }
        self.factories.insert(kind, factory);
        Ok(())
    }

    pub fn resolve(
        &self,
        kind: PageContentKind,
        bytes: &[u8],
    ) -> Result<Box<dyn PageTuple>, StoreError> {
        let factory = self
            .factories
            .get(&kind)
            .ok_or(StoreError::UnknownPageContentKind(kind.0))?;
        factory(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tuple::Tuple;

    #[test]
    fn test_builtin_resolves_any_tuple() {
        let mut page = AnyTuplePage::new();
        page.add(Tuple::new(1, b"hello")).unwrap();
        let bytes = PageTuple::to_bytes(&page).unwrap();

        let registry = PageContentRegistry::builtin();
        let resolved = registry
            .resolve(PageContentKind::ANY_TUPLE, &bytes)
            .unwrap();
        assert_eq!(resolved.to_bytes().unwrap(), bytes);
    }

    #[test]
    fn test_builtin_resolves_fixed_tuple() {
        let mut page = FixedTuplePage::new(64);
        page.add(Tuple::new(1, b"hello")).unwrap();
        let bytes = PageTuple::to_bytes(&page).unwrap();

        let registry = PageContentRegistry::builtin();
        let resolved = registry
            .resolve(PageContentKind::FIXED_TUPLE, &bytes)
            .unwrap();
        assert_eq!(resolved.to_bytes().unwrap(), bytes);
    }

    #[test]
    fn test_resolve_unregistered_kind_errors() {
        let registry = PageContentRegistry::builtin();
        let err = registry.resolve(PageContentKind(99), &[]).unwrap_err();
        assert!(matches!(err, StoreError::UnknownPageContentKind(99)));
    }

    #[test]
    fn test_register_custom_kind() {
        let mut registry = PageContentRegistry::builtin();
        registry
            .register(
                PageContentKind(3),
                Arc::new(|bytes| Ok(Box::new(AnyTuplePage::from_bytes(bytes)?) as Box<dyn PageTuple>)),
            )
            .unwrap();

        let mut page = AnyTuplePage::new();
        page.add(Tuple::new(7, b"world")).unwrap();
        let bytes = PageTuple::to_bytes(&page).unwrap();
        let resolved = registry.resolve(PageContentKind(3), &bytes).unwrap();
        assert_eq!(resolved.to_bytes().unwrap(), bytes);
    }

    #[test]
    fn test_register_duplicate_kind_errors() {
        let mut registry = PageContentRegistry::builtin();
        let err = registry
            .register(
                PageContentKind::ANY_TUPLE,
                Arc::new(|bytes| Ok(Box::new(AnyTuplePage::from_bytes(bytes)?) as Box<dyn PageTuple>)),
            )
            .unwrap_err();
        assert!(matches!(err, StoreError::DuplicatePageContentKind(0)));
    }
}
