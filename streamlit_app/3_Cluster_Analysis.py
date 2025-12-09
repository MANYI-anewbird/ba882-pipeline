import streamlit as st
from google.cloud import bigquery
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

# Page config
st.set_page_config(page_title="Cluster Analysis", page_icon="🤖", layout="wide")

# Connect to BigQuery
@st.cache_resource
def get_bigquery_client():
    return bigquery.Client(project="ba-882-fall25-team8")

client = get_bigquery_client()

# Load data
@st.cache_data(ttl=3600)
def load_and_cluster_data():
    """Load repo data and compute clusters"""
    query = """
    SELECT 
        repo_name,
        repo_url,
        stars_count,
        forks_count,
        watchers,
        open_issues,
        repo_age_days,
        stars_per_day,
        total_commits,
        total_contributors,
        primary_language_pct,
        language_count,
        commits_per_contributor,
        forks_per_star,
        category,
        complexity_level,
        complexity_score
    FROM `ba-882-fall25-team8.cleaned_github_data.repo_ml_ready`
    WHERE stars_count IS NOT NULL
    ORDER BY stars_count DESC
    """
    df = client.query(query).to_dataframe()
    
    # Define feature columns for clustering
    feature_cols = ['stars_count', 'forks_count', 'total_commits', 'total_contributors', 
                    'repo_age_days', 'stars_per_day', 'language_count', 
                    'commits_per_contributor', 'forks_per_star']
    
    # Only use columns that exist and have data
    available_cols = [c for c in feature_cols if c in df.columns]
    X = df[available_cols].fillna(0)
    
    # Scale features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # KMeans clustering
    kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
    df['cluster'] = kmeans.fit_predict(X_scaled)
    
    return df

# Main page
st.title("🤖 ML Cluster Analysis")
st.markdown("---")

# Add tip box
st.info("💡 **Tip:** Explore how machine learning grouped repositories into 4 distinct patterns based on 18 different metrics!")

st.markdown("---")

try:
    # Load data
    with st.spinner("Loading cluster analysis..."):
        df = load_and_cluster_data()
    
    # Cluster names
    cluster_names = {
        0: "Cluster 0: High Activity",
        1: "Cluster 1: Growing Projects",
        2: "Cluster 2: Mature Repos",
        3: "Cluster 3: Niche Projects"
    }
    df['cluster_name'] = df['cluster'].map(cluster_names)
    
    # === SECTION 1: Overview Stats ===
    st.header("📊 Cluster Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    for idx, (cluster_id, name) in enumerate(cluster_names.items()):
        cluster_data = df[df['cluster'] == cluster_id]
        with [col1, col2, col3, col4][idx]:
            st.metric(
                f"🔹 {name}",
                f"{len(cluster_data)} repos",
                delta=f"Avg: {cluster_data['stars_count'].mean():,.0f} ⭐"
            )
    
    st.markdown("---")
    
    # === SECTION 2: Interactive Cluster Visualization ===
    st.header("🎯 Interactive Cluster Visualization")
    
    # Let user choose axes
    col1, col2, col3 = st.columns(3)
    
    numeric_cols = [
        'stars_count', 'forks_count', 'total_commits', 'total_contributors',
        'repo_age_days', 'stars_per_day', 'language_count', 
        'commits_per_contributor', 'forks_per_star'
    ]
    
    # Filter to only available columns
    available_numeric = [c for c in numeric_cols if c in df.columns]
    
    with col1:
        x_axis = st.selectbox(
            "X-Axis:",
            options=available_numeric,
            index=0,
            format_func=lambda x: x.replace('_', ' ').title()
        )
    
    with col2:
        y_axis = st.selectbox(
            "Y-Axis:",
            options=available_numeric,
            index=min(2, len(available_numeric)-1),
            format_func=lambda x: x.replace('_', ' ').title()
        )
    
    with col3:
        size_col = st.selectbox(
            "Bubble Size:",
            options=available_numeric,
            index=min(3, len(available_numeric)-1),
            format_func=lambda x: x.replace('_', ' ').title()
        )
    
    # Create scatter plot
    df['size_for_plot'] = df[size_col].apply(lambda x: max(x, 1) if pd.notna(x) else 1)
    
    fig_clusters = px.scatter(
        df,
        x=x_axis,
        y=y_axis,
        color='cluster_name',
        size='size_for_plot',
        hover_data={
            'repo_name': True,
            x_axis: ':,.0f',
            y_axis: ':,.0f',
            size_col: ':,.0f',
            'cluster_name': True,
            'size_for_plot': False
        },
        labels={
            x_axis: x_axis.replace('_', ' ').title(),
            y_axis: y_axis.replace('_', ' ').title(),
            'cluster_name': 'Cluster'
        },
        title=f'Cluster Distribution: {x_axis.replace("_", " ").title()} vs {y_axis.replace("_", " ").title()}',
        color_discrete_sequence=px.colors.qualitative.Set2,
        height=600
    )
    
    fig_clusters.update_traces(marker=dict(line=dict(width=0.5, color='white')))
    fig_clusters.update_layout(showlegend=True)
    
    st.plotly_chart(fig_clusters, use_container_width=True)
    
    st.markdown("---")
    
    # === SECTION 3: Cluster Comparison Table ===
    st.header("📋 Cluster Comparison")
    st.markdown("*Average metrics across all clusters*")
    
    # Calculate cluster statistics
    comparison_metrics = {
        'Cluster': [],
        'Repos': [],
        'Avg Stars': [],
        'Avg Forks': [],
        'Avg Commits': [],
        'Avg Contributors': [],
        'Avg Age (years)': [],
        'Avg Stars/Day': [],
        'Avg Languages': []
    }
    
    for cluster_id in sorted(df['cluster'].unique()):
        cluster_data = df[df['cluster'] == cluster_id]
        comparison_metrics['Cluster'].append(cluster_names.get(cluster_id, f"Cluster {cluster_id}"))
        comparison_metrics['Repos'].append(len(cluster_data))
        comparison_metrics['Avg Stars'].append(int(cluster_data['stars_count'].mean()))
        comparison_metrics['Avg Forks'].append(int(cluster_data['forks_count'].mean()))
        comparison_metrics['Avg Commits'].append(int(cluster_data['total_commits'].mean()) if 'total_commits' in cluster_data else 0)
        comparison_metrics['Avg Contributors'].append(int(cluster_data['total_contributors'].mean()) if 'total_contributors' in cluster_data else 0)
        comparison_metrics['Avg Age (years)'].append(round(cluster_data['repo_age_days'].mean() / 365, 1) if 'repo_age_days' in cluster_data else 0)
        comparison_metrics['Avg Stars/Day'].append(round(cluster_data['stars_per_day'].mean(), 2) if 'stars_per_day' in cluster_data else 0)
        comparison_metrics['Avg Languages'].append(round(cluster_data['language_count'].mean(), 1) if 'language_count' in cluster_data else 0)
    
    df_comparison = pd.DataFrame(comparison_metrics)
    
    # Display as formatted table
    st.dataframe(
        df_comparison.style.format({
            'Avg Stars': '{:,}',
            'Avg Forks': '{:,}',
            'Avg Commits': '{:,}',
            'Avg Contributors': '{:,}',
            'Avg Stars/Day': '{:.2f}',
            'Avg Languages': '{:.1f}'
        }),
        use_container_width=True,
        hide_index=True
    )
    
    st.markdown("---")
    
    # === SECTION 4: Cluster Characteristics ===
    st.header("🔍 Cluster Characteristics")
    
    # Select cluster to explore
    selected_cluster_name = st.selectbox(
        "Select a cluster to explore in detail:",
        options=sorted(df['cluster_name'].unique())
    )
    
    cluster_id = [k for k, v in cluster_names.items() if v == selected_cluster_name][0]
    cluster_df = df[df['cluster'] == cluster_id].copy()
    
    # Cluster summary
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📦 Repositories", f"{len(cluster_df)}")
    with col2:
        st.metric("⭐ Total Stars", f"{cluster_df['stars_count'].sum():,}")
    with col3:
        st.metric("💻 Total Commits", f"{cluster_df['total_commits'].sum():,}" if 'total_commits' in cluster_df else "N/A")
    with col4:
        st.metric("👥 Total Contributors", f"{cluster_df['total_contributors'].sum():,}" if 'total_contributors' in cluster_df else "N/A")
    
    st.markdown("---")
    
    # Distribution charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("⭐ Stars Distribution")
        fig_stars = px.histogram(
            cluster_df,
            x='stars_count',
            nbins=20,
            labels={'stars_count': 'Stars', 'count': 'Number of Repos'},
            title='',
            color_discrete_sequence=['rgb(0, 123, 255)']
        )
        fig_stars.update_layout(height=300, showlegend=False)
        st.plotly_chart(fig_stars, use_container_width=True)
    
    with col2:
        st.subheader("📅 Age Distribution")
        cluster_df['age_years'] = cluster_df['repo_age_days'] / 365
        fig_age = px.histogram(
            cluster_df,
            x='age_years',
            nbins=20,
            labels={'age_years': 'Age (years)', 'count': 'Number of Repos'},
            title='',
            color_discrete_sequence=['rgb(0, 123, 255)']
        )
        fig_age.update_layout(height=300, showlegend=False)
        st.plotly_chart(fig_age, use_container_width=True)
    
    st.markdown("---")
    
    # Top repos in cluster
    st.subheader(f"🏆 Top 20 Repositories in {selected_cluster_name}")
    
    display_cols = ['repo_name', 'stars_count', 'forks_count', 'total_commits', 'total_contributors', 'language_count', 'stars_per_day']
    available_display = [c for c in display_cols if c in cluster_df.columns]
    
    top_repos = cluster_df.nlargest(20, 'stars_count')[available_display].copy()
    
    if 'stars_per_day' in top_repos.columns:
        top_repos['stars_per_day'] = top_repos['stars_per_day'].round(2)
    
    rename_map = {
        'repo_name': 'Repository',
        'stars_count': 'Stars',
        'forks_count': 'Forks',
        'total_commits': 'Commits',
        'total_contributors': 'Contributors',
        'language_count': 'Languages',
        'stars_per_day': 'Stars/Day'
    }
    
    st.dataframe(
        top_repos.rename(columns={k: v for k, v in rename_map.items() if k in top_repos.columns}),
        use_container_width=True,
        hide_index=True
    )
    
    st.markdown("---")
    
    # === SECTION 5: Feature Importance ===
    st.header("📊 What Defines This Cluster?")
    st.markdown("*Average metrics compared to overall dataset*")
    
    # Calculate z-scores (how much this cluster differs from average)
    features_to_compare = [
        'stars_count', 'forks_count', 'total_commits', 'total_contributors',
        'stars_per_day', 'language_count', 'commits_per_contributor'
    ]
    
    available_features = [f for f in features_to_compare if f in df.columns]
    
    comparison_data = []
    for feature in available_features:
        cluster_avg = cluster_df[feature].mean()
        overall_avg = df[feature].mean()
        overall_std = df[feature].std()
        
        # Calculate z-score
        z_score = (cluster_avg - overall_avg) / overall_std if overall_std > 0 else 0
        
        comparison_data.append({
            'Feature': feature.replace('_', ' ').title(),
            'Cluster Avg': cluster_avg,
            'Overall Avg': overall_avg,
            'Difference (%)': ((cluster_avg - overall_avg) / overall_avg * 100) if overall_avg > 0 else 0,
            'Z-Score': z_score
        })
    
    df_features = pd.DataFrame(comparison_data)
    df_features = df_features.sort_values('Z-Score', key=abs, ascending=False)
    
    # Horizontal bar chart of z-scores
    fig_features = px.bar(
        df_features,
        y='Feature',
        x='Z-Score',
        orientation='h',
        labels={'Z-Score': 'Z-Score (Standard Deviations from Mean)', 'Feature': ''},
        title='How This Cluster Differs from Average',
        color='Z-Score',
        color_continuous_scale='RdBu_r',
        color_continuous_midpoint=0
    )
    fig_features.update_layout(height=400, showlegend=False)
    
    st.plotly_chart(fig_features, use_container_width=True)
    
    st.info("**📌 Interpretation:** Positive values (red) indicate this cluster is above average, negative values (blue) indicate below average.")
    
    # Show detailed comparison table
    with st.expander("📋 View Detailed Comparison"):
        st.dataframe(
            df_features.style.format({
                'Cluster Avg': '{:,.2f}',
                'Overall Avg': '{:,.2f}',
                'Difference (%)': '{:+.1f}%',
                'Z-Score': '{:+.2f}'
            }),
            use_container_width=True,
            hide_index=True
        )

except Exception as e:
    st.error(f"Error loading data: {str(e)}")
    st.info("Make sure all required BigQuery tables exist and have data.")