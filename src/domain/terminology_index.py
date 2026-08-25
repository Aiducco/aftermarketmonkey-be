"""
Stage G: in-memory retrieval index over PCdb terminology names + aliases. Pure -- built once
from already-fetched rows (no Django ORM inside the index itself, matching title_mask.py's
VcdbLookup convention), rebuildable whenever PCdb updates (see load_pcdb).

Tokenisation has three load-bearing rules, all found to matter on real data:
- Stopwords exclude "and"/"or"/"for"/"the"/"with"/"w"/"assembly" but deliberately NOT "kit" or
  "set" -- those are discriminative ("Suspension Lift Kit" vs "...Bracket Kit").
- Stemming cuts a trailing plural s when len(token) > 3, not > 4 -- the off-by-one matters:
  len > 4 makes "mats" (4 chars) unstemmable, so "Floor Mat" (PCdb's actual singular form)
  becomes unreachable from a query built off "floor mats".
  # >>> len("mats") > 4  ->  False (bug); len("mats") > 3 -> True (correct)
- Compound splitting rescues manufacturer coinages PCdb never contains as one word
  ("floorliner" -> "floor" + "liner", "sillprotector" -> "sill" + "protector") by checking if a
  long unrecognized token splits into two known-vocabulary halves.
"""
import math
import re

STOP = {"and", "or", "for", "the", "with", "w", "assembly"}
DF_CUTOFF = 4000
# Not given as an exact number in the source spec -- tunable like MIN_GROUP_SIZE elsewhere in
# this pipeline. Reward is added once if the terminology's own head noun (last token) appears
# anywhere in the query, on top of whatever IDF-weighted overlap already contributed.
HEAD_NOUN_WEIGHT = 1.0
SHORTLIST_K = 8
# The source spec's own given pseudocode uses len(t) < 8 here -- but then lists "sinkmat" (7
# chars) as a case compound splitting should rescue, which its own threshold can't reach. Same
# class of off-by-one as the len>4 stemming bug it explicitly flags elsewhere ("observed, not
# hypothetical"); lowered by the same reasoning, verified against real PCdb vocabulary below.
MIN_COMPOUND_LEN = 6
COMPOUND_SPLIT_MARGIN = 3  # don't try splits that leave a fragment shorter than this

_TOKEN_RE = re.compile(r"[a-z0-9]+")


def tokenize_raw(text: str) -> list:
    return _TOKEN_RE.findall((text or "").lower())


def stem(token: str) -> str:
    if len(token) > 3 and token.endswith("s") and not token.endswith("ss"):
        return token[:-1]
    return token


def split_compound(token: str, vocab: set) -> list:
    """'floorliner' -> ['floor', 'liner'] if both halves are known vocabulary words; returns
    [token] unchanged if it's already known, too short to bother, or no valid split exists."""
    if token in vocab or len(token) < MIN_COMPOUND_LEN:
        return [token]
    for i in range(COMPOUND_SPLIT_MARGIN, len(token) - COMPOUND_SPLIT_MARGIN + 1):
        a, b = stem(token[:i]), stem(token[i:])
        if a in vocab and b in vocab:
            return [a, b]
    return [token]


def tokenize(text: str, vocab: set) -> list:
    """Full pipeline: raw split -> stopword removal -> stem -> compound split (which itself
    stems each half). vocab is the corpus-wide set of known single-word stems, built in a first
    pass over every terminology name+alias before this function is used for real (see
    TerminologyIndex.__init__)."""
    tokens = []
    for raw in tokenize_raw(text):
        if raw in STOP:
            continue
        stemmed = stem(raw)
        tokens.extend(split_compound(stemmed, vocab))
    return tokens


class TerminologyIndex:
    """
    terminology_rows: iterable of dicts/objects with part_terminology_id, name, aliases (list
    of strings), is_active -- e.g. PcdbTerminologyFlat.objects.values(...). Inactive
    terminologies are excluded entirely; never classify to a superseded term.
    """

    def __init__(self, terminology_rows):
        rows = [r for r in terminology_rows if r["is_active"]]

        # Pass 1: build the corpus vocabulary from raw single-token stems only (pre-compound-
        # split) -- this is what split_compound checks candidate halves against, so a token
        # only counts as "known vocabulary" if it appears somewhere on its own, not already as
        # half of another compound guess.
        vocab = set()
        for r in rows:
            for raw in tokenize_raw(r["name"]) + [t for a in (r["aliases"] or []) for t in tokenize_raw(a)]:
                if raw not in STOP:
                    vocab.add(stem(raw))
        self.vocab = vocab

        # Pass 2: tokenize every terminology's full text (name + aliases) for real, build the
        # inverted index, and record each terminology's own token set + head noun for scoring.
        self.postings = {}  # token -> set(part_terminology_id)
        self.term_tokens = {}  # part_terminology_id -> set(token)
        self.term_head_noun = {}  # part_terminology_id -> str (last raw-name token, post-stem)
        self.term_meta = {}  # part_terminology_id -> row (for building candidate payloads later)

        for r in rows:
            tid = r["part_terminology_id"]
            name_tokens = tokenize(r["name"], vocab)
            alias_tokens = [t for a in (r["aliases"] or []) for t in tokenize(a, vocab)]
            all_tokens = set(name_tokens) | set(alias_tokens)
            self.term_tokens[tid] = all_tokens
            self.term_head_noun[tid] = name_tokens[-1] if name_tokens else None
            self.term_meta[tid] = r
            for tok in all_tokens:
                self.postings.setdefault(tok, set()).add(tid)

        n = len(rows)
        self.n_terminologies = n
        self.idf = {}
        for tok, ids in self.postings.items():
            df = len(ids)
            if df > DF_CUTOFF:
                continue
            self.idf[tok] = math.log(n / df) if df else 0.0

    def query_tokens(self, text: str) -> list:
        return tokenize(text, self.vocab)

    def search(self, query_text: str, k: int = SHORTLIST_K) -> list:
        """Returns up to k (part_terminology_id, score) tuples, highest score first. query_text
        should be the group's full context (head name + residues + example titles), not just
        the head name alone -- a bare "rubber mats" query misses "Floor Mat" entirely since the
        query never mentions "floor"; richer context supplies that missing vocabulary."""
        q_tokens = set(self.query_tokens(query_text))
        if not q_tokens:
            return []

        candidate_ids = set()
        for tok in q_tokens:
            candidate_ids |= self.postings.get(tok, set())

        scored = []
        for tid in candidate_ids:
            term_tokens = self.term_tokens[tid]
            overlap = q_tokens & term_tokens
            base = sum(self.idf.get(tok, 0.0) for tok in overlap)
            if base <= 0:
                continue
            score = base / (0.5 + 0.5 * len(term_tokens))
            if self.term_head_noun.get(tid) in q_tokens:
                score += HEAD_NOUN_WEIGHT
            scored.append((tid, score))

        scored.sort(key=lambda x: -x[1])
        return scored[:k]
