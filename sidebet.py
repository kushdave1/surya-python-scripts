import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch

# Define the flowchart elements
elements = {
    'User A': {
        'createBet': 'Place Bet\n& Deposit ETH',
        'unfilledBet': 'Store & Track\nUnfilled Bet'
    },
    'Smart Contract': {
        'acceptBet': 'Accept Bet\n& Lock Deposit',
        'resolveBet': 'Resolve Bet\n(Chainlink Oracle)'
    },
    'Player B': {
        'filledBet': 'Bet Filled',
        'withdrawWinnings': 'Withdraw\nWinnings'
    },
    'Stream Over': {
        'notifyWinner': 'Notify Winner',
        'ethTransfer': 'ETH Transfer'
    },
    'Chainlink Oracle': {
        'checkGameStatus': 'Check if\nGame is Over',
        'checkWinner': 'Check Winner'
    },
    'SideBet Company': {
        'collectPercentage': 'Collect Percentage\nof Bet'
    }
}

# Define the connections between elements
connections = [
    ('User A', 'createBet', 'Smart Contract'),
    ('User A', 'unfilledBet', 'Smart Contract'),
    ('Smart Contract', 'acceptBet', 'Player B'),
    ('Smart Contract', 'resolveBet', 'Stream Over'),
    ('Player B', 'filledBet', 'Smart Contract'),
    ('Player B', 'withdrawWinnings', 'Smart Contract'),
    ('Stream Over', 'notifyWinner', 'Player B'),
    ('Stream Over', 'ethTransfer', 'SideBet Company'),
    ('Chainlink Oracle', 'checkGameStatus', 'Stream Over'),
    ('Chainlink Oracle', 'checkWinner', 'Smart Contract'),
    ('SideBet Company', 'collectPercentage', 'Smart Contract')
]

# Create a new figure with adjusted size
fig = plt.figure(figsize=(12, 10))
ax = fig.add_subplot(111)
ax.set_axis_off()

# Set styling parameters
node_width = 0.3
node_height = 0.15
node_padding = 0.2
arrow_style = {'arrowstyle': '->', 'color': 'black'}

# Compute x and y positions for each element
x_positions = {element: i * (node_width + node_padding) for i, element in enumerate(elements.keys())}
y_positions = {element: i * (node_height + node_padding) for i, element in enumerate(elements.keys())}

# Draw the elements and connections
for i, element in enumerate(elements.keys()):
    funcs = elements[element]
    x = x_positions[element]  # x-coordinate for the element node
    y = y_positions[element]  # y-coordinate for the element node

    # Draw the element node
    ax.add_patch(plt.Rectangle((x, y), node_width, node_height, fc='white', ec='black', lw=1.5))
    ax.text(x + node_width / 2, y + node_height / 2, element, ha='center', va='center')

    # Draw the functions within the element node
    for j, func in enumerate(funcs.keys()):
        fx = x + (j + 1) * (node_width / (len(funcs) + 1))
        fy = y + node_height / 2

        ax.text(fx, fy, funcs[func], ha='center', va='center')

        # Draw arrows connecting the functions within the element node
        if j < len(funcs) - 1:
            ax.annotate('', xy=(fx, fy), xytext=(fx + node_width / (len(funcs) + 1), fy), arrowprops=arrow_style)

    # Draw arrows connecting the element node to other elements
    for conn in connections:
        if conn[0] == element:
            ax.annotate('', xy=(x + node_width, y + node_height / 2), xytext=(x + node_width + 0.1, y + node_height / 2),
                        arrowprops=arrow_style)
            ax.annotate('', xy=(x + node_width + 0.1, y + node_height / 2),
                        xytext=(x + node_width + 0.1, y_positions[conn[2]] + node_height / 2), arrowprops=arrow_style)

# Adjust the plot appearance
ax.set_xlim(-0.5, len(elements) * (node_width + node_padding) - 0.5)
ax.set_ylim(-0.5, len(elements) * (node_height + node_padding) - 0.5)
ax.axis('off')

# Save the flowchart as a PDF
plt.savefig('sidebet_smart_contract_flowchart.pdf', format='pdf', bbox_inches='tight')

# Show the flowchart (optional)
plt.show()