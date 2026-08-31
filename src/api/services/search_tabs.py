"""
The tab strip shared by tire, wheel and parts search.

One module so the three modes cannot disagree about what tabs exist, what they are called or when
they show. Before this, tires owned the strip and hard-coded two entries, which is why a wheel
search still rendered "Tires | Parts" and the client had to fake a Wheels tab.

**Only modes with hits appear**, and a strip with a single tab is suppressed entirely -- a part
number search should render no tab strip at all, exactly as it does today.
"""
import typing

MODE_TIRES = "tires"
MODE_WHEELS = "wheels"
MODE_PARTS = "parts"

# Display order, independent of which mode is active or how many hits each has.
TAB_ORDER: typing.Tuple[typing.Tuple[str, str], ...] = (
    (MODE_TIRES, "Tires"),
    (MODE_WHEELS, "Wheels"),
    (MODE_PARTS, "Parts"),
)


def build_tabs(counts: typing.Mapping[str, int], active: str) -> typing.List[typing.Dict[str, typing.Any]]:
    """
    ``counts`` maps mode -> total hits. A mode absent from it, or with zero, is not a tab.

    A lone tab is dropped: it tells the user nothing and takes vertical space on a page that has
    only one kind of result anyway.
    """
    tabs = [
        {"mode": mode, "label": label, "count": counts.get(mode, 0), "active": mode == active}
        for mode, label in TAB_ORDER
        if counts.get(mode, 0) > 0
    ]
    return tabs if len(tabs) > 1 else []
