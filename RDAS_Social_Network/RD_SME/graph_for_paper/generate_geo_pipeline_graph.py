import os
import sys

OUTPUT_DIR = "data/processed/figures"
BASENAME = "geo_pipeline"


def ensure_output_dir() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def build_graph():
    import graphviz

    dot = graphviz.Digraph("Geographic_Normalization", comment="Geographic Normalization Pipeline")
    dot.attr(rankdir="TB", size="10", splines="ortho", nodesep="0.6", ranksep="0.6")

    # Default styles
    dot.attr("node", shape="box", style="rounded,filled", fillcolor="#ffffff", fontname="Helvetica", fontsize="12", penwidth="1.5")
    dot.attr("edge", fontname="Helvetica", fontsize="10")

    # 1. Input Node
    dot.node("A", 'Raw Affiliation String\n(e.g., "Dept. of Pediatrics, Seattle, WA, USA")', fillcolor="#E6F3FF", shape="note")

    # 2. Extraction Step
    dot.node("B", "Entity Extraction\n(spaCy NER)", shape="component")

    # 3. Country Normalization
    dot.node("C", "Country Normalization\n(pycountry / geonamescache)")

    # 4. Logic Branch (US vs Non-US)
    dot.node("D", "Is location in US?", shape="diamond", fillcolor="#FFF2CC")

    # 5. US Path
    with dot.subgraph(name="cluster_US") as c:
        c.attr(label="U.S. Validation", style="dashed", color="grey")
        c.node("E", "City/State Validation\n(Reference US Cities)", shape="box")
        c.node("F", "FIPS Resolution\n(County Level)", shape="box")

    # 6. Non-US Path
    with dot.subgraph(name="cluster_INTL") as c:
        c.attr(label="International Validation", style="dashed", color="grey")
        c.node("G", "City Validation\n(Country-Specific Gazetteers)", shape="box")

    # 7. Quality Control
    dot.node("H", "Quality Control\n(Filter ambiguous/unresolved)", shape="octagon", fillcolor="#FFE6E6")

    # 8. Output
    dot.node("I", "Knowledge Graph Integration\n(Link: City, State, Country, FIPS)", fillcolor="#E6F3FF", shape="cylinder")

    # Edges
    dot.edge("A", "B")
    dot.edge("B", "C", label="Identify Country")
    dot.edge("C", "D")
    dot.edge("D", "E", label="Yes")
    dot.edge("D", "G", label="No")
    dot.edge("E", "F")
    dot.edge("F", "H")
    dot.edge("G", "H")
    dot.edge("H", "I", label="Validated Attributes")

    return dot


def render_graph(dot) -> None:
    # Save DOT source for reference
    dot_path = os.path.join(OUTPUT_DIR, f"{BASENAME}.dot")
    with open(dot_path, "w", encoding="utf-8") as f:
        f.write(dot.source)

    # Try to render PNG and SVG
    try:
        dot.render(filename=os.path.join(OUTPUT_DIR, BASENAME), format="png", cleanup=True)
        dot.render(filename=os.path.join(OUTPUT_DIR, BASENAME), format="svg", cleanup=True)
    except Exception as e:
        msg = (
            f"Failed to render with Graphviz backend: {e}\n"
            "Tip: Ensure Graphviz binaries are installed and on PATH.\n"
            "conda install -y -c conda-forge graphviz python-graphviz\n"
        )
        raise RuntimeError(msg)


def main() -> None:
    ensure_output_dir()
    try:
        dot = build_graph()
    except ModuleNotFoundError as e:
        print("ERROR: Missing python-graphviz package.", file=sys.stderr)
        print("Install via: conda install -y -c conda-forge python-graphviz", file=sys.stderr)
        raise
    render_graph(dot)
    print(f"Wrote: {os.path.join(OUTPUT_DIR, BASENAME)}.png and .svg")


if __name__ == "__main__":
    main()
