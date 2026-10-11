/// Keeps keys in the order that they were first set, as the reference's `Map`
/// does: restoration and resends follow registration order. It holds the
/// segments of one channel, so a linear scan is fast enough.
pub(crate) struct OrderedMap<Value> {
    entries: Vec<(String, Value)>,
} // end struct OrderedMap

impl<Value> Default for OrderedMap<Value> {
    fn default() -> OrderedMap<Value> {
        OrderedMap {
            entries: Vec::new(),
        }
    } // end fn default
} // end impl Default for OrderedMap

impl<Value> OrderedMap<Value> {
    /// Sets the value. A key that is already present keeps its place.
    pub(crate) fn set(&mut self, key: &str, value: Value) {
        match self
            .entries
            .iter_mut()
            .find(|(existing, _)| existing == key)
        {
            Some(entry) => entry.1 = value,
            None => self.entries.push((key.to_owned(), value)),
        }
    } // end method set

    pub(crate) fn get(&self, key: &str) -> Option<&Value> {
        self.entries
            .iter()
            .find(|(existing, _)| existing == key)
            .map(|(_, value)| value)
    } // end method get

    pub(crate) fn remove(&mut self, key: &str) {
        self.entries.retain(|(existing, _)| existing != key);
    } // end method remove

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&str, &Value)> {
        self.entries
            .iter()
            .map(|(key, value)| (key.as_str(), value))
    } // end method iter

    pub(crate) fn keys(&self) -> Vec<String> {
        self.entries.iter().map(|(key, _)| key.clone()).collect()
    } // end method keys

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    } // end method is_empty

    pub(crate) fn clear(&mut self) {
        self.entries.clear();
    } // end method clear
} // end impl OrderedMap
