import matplotlib.pyplot as plt
import numpy as np

# ==========================================
# 1. DATA INPUT
# ==========================================

# Dataset 1: Customer (Cus)
# -------------------------
# App 1: Replace (Filter + Transform)
spark_cus_replace = [556.57, 602.36, 609.69]
rs_cus_replace    = [5.17, 8.17, 5.06]  # Calculated from your logs

# App 2: Aggregate (Filter + AggByKey)
spark_cus_agg     = [26.98, 25.27, 26.40]
rs_cus_agg        = [27.12, 27.78, 28.03] # Calculated from your logs

# Dataset 2: GIS
# -------------------------
# App 3: Warning (Filter + Identity)
spark_gis_warning = [2543.00, 2774.38, 3059.71]
rs_gis_warning    = [23.70, 25.82, 21.73] # Calculated from your logs

# App 4: School (Filter + AggByKey)
spark_gis_school  = [1.52, 1.74, 1.73]
rs_gis_school     = [19.06, 20.52, 19.83] # Calculated from your logs

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================

def get_stats(data):
    """Returns mean and std dev."""
    if not data: return 0, 0
    return np.mean(data), np.std(data)

# Prepare data structure for plotting
experiments = [
    {
        "title": "Customer: Filter & Replace\n(grep:India -> replace:Bharat)",
        "spark": spark_cus_replace,
        "rainstorm": rs_cus_replace
    },
    {
        "title": "Customer: Aggregation\n(grep:LLC -> agg:Country)",
        "spark": spark_cus_agg,
        "rainstorm": rs_cus_agg
    },
    {
        "title": "GIS: Sign Inspection\n(grep:Warning -> identity)",
        "spark": spark_gis_warning,
        "rainstorm": rs_gis_warning
    },
    {
        "title": "GIS: School Count\n(grep:School -> agg:City)",
        "spark": spark_gis_school,
        "rainstorm": rs_gis_school
    }
]

# ==========================================
# 3. PLOTTING LOGIC
# ==========================================

# Setup figure with 2x2 grid
fig, axes = plt.subplots(2, 2, figsize=(14, 10))
fig.suptitle('RainStorm vs. Spark Streaming Performance Comparison', fontsize=16, weight='bold')
axes = axes.flatten()

# Colors
color_spark = '#2ca02c'    # Green
color_rs = '#1f77b4'       # Blue

for i, exp in enumerate(experiments):
    ax = axes[i]

    # Calculate stats
    s_mean, s_std = get_stats(exp['spark'])
    r_mean, r_std = get_stats(exp['rainstorm'])

    means = [r_mean, s_mean]
    stds  = [r_std, s_std]
    labels = ['RainStorm', 'Spark']

    # Create Bar Chart
    x_pos = np.arange(len(labels))
    bars = ax.bar(x_pos, means, yerr=stds, align='center', alpha=0.9,
                  color=[color_rs, color_spark], capsize=10, width=0.6)

    # Formatting
    ax.set_title(exp['title'], fontsize=12, fontweight='bold', pad=15)
    ax.set_xticks(x_pos)
    ax.set_xticklabels(labels, fontsize=11)
    ax.set_ylabel('Throughput (Records/Sec)', fontsize=10)
    ax.yaxis.grid(True, linestyle='--', alpha=0.7)

    # Logarithmic scale if differences are massive (optional, currently linear)
    # ax.set_yscale('log')

    # Add numeric labels on top of bars
    for bar in bars:
        height = bar.get_height()
        ax.annotate(f'{height:.2f}',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 5),  # vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom', fontsize=10, fontweight='bold')

plt.tight_layout(rect=[0, 0.03, 1, 0.95])
plt.savefig('performance_comparison.png', dpi=300)
print("Graph generated successfully: performance_comparison.png")
plt.show()