import rustworkx as rx


def topological_layout(dag: rx.PyDAG) -> dict[int, tuple[float, float]]:
    return {
        node_index: (
            float(generation_index),
            node_position - (len(generation) - 1) / 2,
        )
        for generation_index, generation in enumerate(rx.topological_generations(dag))
        for node_position, node_index in enumerate(generation)
    }
