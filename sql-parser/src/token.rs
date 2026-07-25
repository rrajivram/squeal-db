use crate::{keyword::Keyword, span::TokenSpan};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Token<'src> {
    Decimal {
        is_neg: bool,
        part1: u64,
        period: bool,
        part2: u64,
    },
    Word {
        raw: &'src str,
        keyword: Option<Keyword>,
    },
    String {
        raw: &'src str,
        kind: StringStyle,
    },
    Space,
    Punctuation(Punctuation),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TokenStruct<'src> {
    pub token: Token<'src>,
    pub span: TokenSpan,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum StringStyle {
    SingleQuoted(Option<char>),
    DoubleQuoted(Option<char>),
}

impl<'src> Token<'src> {
    pub fn is_whitespace(c: char) -> bool {
        matches!(c, ' ' | '\r' | '\n' | '\t')
    }
}

impl<'src> PartialEq<TokenStruct<'src>> for Token<'src> {
    fn eq(&self, other: &TokenStruct<'src>) -> bool {
        match (self, &other.token) {
            (
                Token::Decimal {
                    is_neg: a_isneg,
                    part1: a_part1,
                    period: a_per,
                    part2: a_part2,
                },
                Token::Decimal {
                    is_neg: b_isneg,
                    part1: b_part1,
                    period: b_per,
                    part2: b_part2,
                },
            ) => {
                *a_isneg == *b_isneg
                    && *a_per == *b_per
                    && *a_part1 == *b_part1
                    && *a_part2 == *b_part2
            }
            (
                Token::Word {
                    raw: a_raw,
                    keyword: a_key,
                },
                Token::Word {
                    raw: b_raw,
                    keyword: b_key,
                },
            ) => *a_raw == *b_raw && *a_key == *b_key,
            (
                Token::String {
                    raw: a_raw,
                    kind: a_kind,
                },
                Token::String {
                    raw: b_raw,
                    kind: b_kind,
                },
            ) => *a_raw == *b_raw && *a_kind == *b_kind,
            (Token::Space, Token::Space) => true,
            (Token::Punctuation(punctuation_a), Token::Punctuation(punctuation_b)) => {
                *punctuation_a == *punctuation_b
            }
            (_, _) => false,
        }
    }
}

macro_rules! create_punctuation {
    ($(($x:expr,$c:literal,$i:ident)),*$(,)?) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq,Hash)]
        pub enum Punctuation {
            $($i,)*
        }

        impl Punctuation {
            pub fn from_char(c: char) -> Option<Self> {
                match c {
                    $($c => Some(Self::$i),)*
                    _ => None,
                }
            }

            pub fn to_char(self) -> char {
                match self {
                    $(Self::$i => $c,)*
                }
            }
        }
    };
}

create_punctuation!(
    (0x21, '!', ExclamationMark),
    (0x23, '#', NumberSign),
    (0x24, '$', Dollar),
    (0x25, '%', Percent),
    (0x26, '&', Ampersand),
    (0x28, '(', LeftParenthesis),
    (0x29, ')', RightParenthesis),
    (0x2A, '*', Asterisk),
    (0x2B, '+', Plus),
    (0x2C, ',', Comma),
    (0x2D, '-', Minus),
    (0x2E, '.', Period),
    (0x2F, '/', Slash),
    (0x3A, ':', Colon),
    (0x3B, ';', Semicolon),
    (0x3C, '<', LessThan),
    (0x3D, '=', Equals),
    (0x3E, '>', GreaterThan),
    (0x3F, '?', QuestionMark),
    (0x40, '@', At),
    (0x5B, '[', LeftBracket),
    (0x5C, '\\', Backslash),
    (0x5D, ']', RightBracket),
    (0x5E, '^', Caret),
    (0x7B, '{', LeftBrace),
    (0x7C, '|', VerticalBar),
    (0x7D, '}', RightBrace),
    (0x7E, '~', Tilde),
);
