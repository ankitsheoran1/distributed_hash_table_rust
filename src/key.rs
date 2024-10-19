use sha2::{Digest, Sha256};
use serde::{Deserialize, Serialize};

pub const KEY_LEN: usize = 32;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Debug)]
pub struct Key(pub [u8; KEY_LEN]);

impl Key {
    pub fn new(input: String) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(input.as_bytes());
        let result = hasher.finalize();
        let mut hash = [0; KEY_LEN];

        for i in 0..result.len() {
            hash[i] = result[i];
        }

        Self(hash)
    }
}


#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_key_creation() {
        let input = "test input".to_string();
        let key = Key::new(input);
        let input1 = "test input1".to_string();
        let key1 = Key::new(input1);
        for i in 0..KEY_LEN {
            println!("***********{:?}******", key1.0[i])
        }
        assert_eq!(key.0.len(), KEY_LEN, "Key length does not match KEY_LEN");
    }
}
