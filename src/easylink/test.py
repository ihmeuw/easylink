import networkx as nx


def is_consistent_with_topological_order(G, node_list):
    # Get the subgraph induced by the nodes in node_list

    # Check if there's a path between each pair of nodes in the order they appear
    for i in range(len(node_list)):
        for j in range(i + 1, len(node_list)):
            if nx.has_path(G, node_list[j], node_list[i]):
                return False

    return True


# Example usage:
if __name__ == "__main__":
    # Create a simple directed acyclic graph
    G = nx.DiGraph()
    G.add_edges_from([(1, 2), (1, 3), (2, 4), (3, 4), (4, 5), (3, 6)])

    # Test cases
    print(is_consistent_with_topological_order(G, [1, 2, 3, 4, 5]))  # Should return True
    print(is_consistent_with_topological_order(G, [1, 3, 2, 4, 5]))  # Should return True
    print(is_consistent_with_topological_order(G, [2, 1, 3, 4, 5]))  # Should return False
    print(is_consistent_with_topological_order(G, [1, 2, 5]))  # Should return True
    print(is_consistent_with_topological_order(G, [1, 5, 6, 3]))  # Should return False
