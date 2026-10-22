//! Redis's glob-style patterns (`KEYS`, `SCAN ... MATCH`): a port of
//! `stringmatchlen` from Redis 7.0's `util.c`, quirks included: `*` any
//! run, `?` one byte, `[abc]`, `[^abc]`, `[a-z]` (reversed ranges work,
//! bytes compared as C's signed `char`), `\x` a literal `x`; an unclosed
//! `[` ends the set at the end of the pattern.

/// Whether `string` matches `pattern`. Like Redis's, `*` doesn't match an
/// empty string: callers take a lone `*` as matching everything.
pub fn glob_match(pattern: &[u8], string: &[u8]) -> bool {
    let mut skip_longer = false;
    matches(pattern, string, &mut skip_longer)
}

/// `pattern[i]`, or 0 past its end (where C would read the terminator).
fn at(pattern: &[u8], i: usize) -> u8 {
    pattern.get(i).copied().unwrap_or(0)
}

/// C's `char` on x86: signed.
fn signed(b: u8) -> i32 {
    i32::from(b as i8)
}

fn matches(pattern: &[u8], string: &[u8], skip_longer: &mut bool) -> bool {
    let (mut p, mut s) = (0, 0);
    while p < pattern.len() && s < string.len() {
        match pattern[p] {
            b'*' => {
                while p + 1 < pattern.len() && pattern[p + 1] == b'*' {
                    p += 1;
                }
                if p + 1 == pattern.len() {
                    return true;
                }
                while s < string.len() {
                    if matches(&pattern[p + 1..], &string[s..], skip_longer) {
                        return true;
                    }
                    if *skip_longer {
                        return false;
                    }
                    s += 1;
                }
                // No match for the rest anywhere in the rest of the string:
                // a longer match for an earlier `*` can't help either.
                *skip_longer = true;
                return false;
            }
            b'?' => s += 1,
            b'[' => {
                p += 1;
                let not = at(pattern, p) == b'^';
                if not {
                    p += 1;
                }
                let c = string[s];
                let mut found = false;
                loop {
                    if at(pattern, p) == b'\\' && pattern.len() - p >= 2 {
                        p += 1;
                        if pattern[p] == c {
                            found = true;
                        }
                    } else if at(pattern, p) == b']' {
                        break;
                    } else if p >= pattern.len() {
                        // Unclosed: the set ends with the pattern.
                        p -= 1;
                        break;
                    } else if pattern.len() - p >= 3 && pattern[p + 1] == b'-' {
                        let (mut lo, mut hi) = (signed(pattern[p]), signed(pattern[p + 2]));
                        if lo > hi {
                            std::mem::swap(&mut lo, &mut hi);
                        }
                        p += 2;
                        if (lo..=hi).contains(&signed(c)) {
                            found = true;
                        }
                    } else if pattern[p] == c {
                        found = true;
                    }
                    p += 1;
                }
                if found == not {
                    return false;
                }
                s += 1;
            }
            b'\\' if pattern.len() - p >= 2 => {
                p += 1;
                if pattern[p] != string[s] {
                    return false;
                }
                s += 1;
            }
            b => {
                if b != string[s] {
                    return false;
                }
                s += 1;
            }
        }
        p += 1;
        if s == string.len() {
            while at(pattern, p) == b'*' {
                p += 1;
            }
            break;
        }
    }
    p >= pattern.len() && s == string.len()
}

#[cfg(test)]
mod tests {
    use super::glob_match;

    #[test]
    fn like_redis() {
        let yes: &[(&str, &str)] = &[
            ("*", "abc"),
            ("a*", "abc"),
            ("*c", "abc"),
            ("a*c", "ac"),
            ("a**c", "abbbc"),
            ("?bc", "abc"),
            ("h[ae]llo", "hello"),
            ("h[^e]llo", "hallo"),
            ("h[a-b]llo", "hbllo"),
            ("h[b-a]llo", "hallo"),
            ("h\\*llo", "h*llo"),
            ("h[\\]]llo", "h]llo"),
            ("[a", "a"),
            ("abc\\", "abc\\"),
            ("user:*:name", "user:42:name"),
            ("", ""),
        ];
        let no: &[(&str, &str)] = &[
            // As in Redis: KEYS and SCAN take a lone `*` as "everything"
            // without asking, so an empty key still shows.
            ("*", ""),
            ("", "a"),
            ("a", ""),
            ("?", ""),
            ("a*c", "ab"),
            ("h[ae]llo", "hillo"),
            ("h[^e]llo", "hello"),
            ("h\\*llo", "hello"),
            ("abc", "abcd"),
            ("abcd", "abc"),
            ("a[", "a"),
            ("a[", "ab"),
            ("a*b*c*d", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        ];
        for (p, s) in yes {
            assert!(glob_match(p.as_bytes(), s.as_bytes()), "{p:?} {s:?}");
        }
        for (p, s) in no {
            assert!(!glob_match(p.as_bytes(), s.as_bytes()), "{p:?} {s:?}");
        }
    }

    #[test]
    fn ranges_compare_signed_bytes() {
        // C's char is signed: 0xff is -1, below 'a'.
        assert!(glob_match(b"[\xff-a]", b"\x00"));
        assert!(!glob_match(b"[a-\xff]", b"b"));
    }
}
