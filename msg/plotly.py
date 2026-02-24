import pandas as pd
import plotly.graph_objects as go
import networkx as nx

# Load your CSV
df = pd.read_csv("your_lineage.csv")  # columns: stage_seq_no, source_table, dest_table

# Build directed graph
G = nx.DiGraph()
for _, row in df.iterrows():
    src = row['source_table']
    dst = row['dest_table']
    stage = row['stage_seq_no']
    G.add_edge(src, dst, stage=stage)

# Get nodes and assign y-position based on stage (higher stage = lower on plot for top-to-bottom flow)
stages = df['stage_seq_no'].unique()
stage_to_y = {stage: -i for i, stage in enumerate(sorted(stages))}  # negative so earlier stages at top

pos = nx.drawing.nx_agraph.graphviz_layout(G, prog="dot")  # or "dagre" if you install pygraphviz
# Fallback: manual y by stage, x spread out
if pos is None:
    pos = {}
    for node in G.nodes():
        outgoing_stages = [G.edges[e]['stage'] for e in G.out_edges(node)]
        pos[node] = (len(G.in_edges(node)) - len(G.out_edges(node)),  # rough x spread
                     stage_to_y[max(outgoing_stages) if outgoing_stages else 0])

# Prepare Plotly traces
edge_x, edge_y = [], []
for edge in G.edges():
    x0, y0 = pos[edge[0]]
    x1, y1 = pos[edge[1]]
    edge_x.extend([x0, x1, None])
    edge_y.extend([y0, y1, None])

edge_trace = go.Scatter(x=edge_x, y=edge_y, line=dict(width=2, color='#888'), hoverinfo='none', mode='lines')

node_x, node_y, node_text, node_color = [], [], [], []
for node in G.nodes():
    x, y = pos[node]
    node_x.append(x)
    node_y.append(y)
    node_text.append(node)
    # Color by in-degree/out-degree or stage
    node_color.append(len(G.out_edges(node)))  # e.g., darker = more downstream impact

node_trace = go.Scatter(
    x=node_x, y=node_y,
    mode='markers+text',
    hoverinfo='text',
    text=node_text,
    textposition="top center",
    marker=dict(size=20, color=node_color, colorscale='Viridis', line_width=2),
    hovertemplate="%{text}<br>Downstream: %{marker.color} tables<extra></extra>"
)

fig = go.Figure(data=[edge_trace, node_trace],
                layout=go.Layout(
                    title='BigQuery Table Lineage (Flow Top → Bottom)',
                    showlegend=False,
                    hovermode='closest',
                    margin=dict(b=20, l=5, r=5, t=40),
                    xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                    yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                    height=800
                ))

fig.show()           # opens in browser
# fig.write_html("lineage_plotly.html")  # save standalone interactive HTML
