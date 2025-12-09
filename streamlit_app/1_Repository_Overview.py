import streamlit as st
from google.cloud import bigquery
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

# Page config
st.set_page_config(page_title="Repository Overview", page_icon="📊", layout="wide")

# Connect to BigQuery
@st.cache_resource
def get_bigquery_client():
    return bigquery.Client(project="ba-882-fall25-team8")

client = get_bigquery_client()

# Load data functions
@st.cache_data(ttl=3600)
def load_repo_data():
    """Load ML-ready repo data with all features"""
    query = """
    SELECT 
        repo_name,
        repo_url,
        main_language,
        stars_count,
        forks_count,
        watchers,
        open_issues,
        repo_age_days,
        stars_per_day,
        total_commits,
        total_contributors,
        primary_language,
        primary_language_pct,
        language_count,
        commits_per_contributor,
        forks_per_star,
        category,
        complexity_level,
        complexity_score,
        audience,
        use_cases
    FROM `ba-882-fall25-team8.cleaned_github_data.repo_ml_ready`
    WHERE stars_count IS NOT NULL
    ORDER BY stars_count DESC
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_language_data():
    """Load language summary data"""
    query = """
    SELECT 
        repo_full_name,
        primary_language,
        language_count,
        top_5_languages,
        languages_above_10_percent,
        secondary_language,
        tertiary_language,
        has_web_markup,
        has_javascript,
        has_scripting_language,
        has_compiled_language
    FROM `ba-882-fall25-team8.cleaned_github_data.language_summary`
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def compute_clusters(df):
    """Compute KMeans clusters on the fly"""
    feature_cols = ['stars_count', 'forks_count', 'total_commits', 'total_contributors', 
                    'repo_age_days', 'stars_per_day', 'language_count']
    
    # Prepare features - only use columns that exist
    available_cols = [c for c in feature_cols if c in df.columns]
    X = df[available_cols].fillna(0)
    
    # Scale
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # KMeans
    kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
    clusters = kmeans.fit_predict(X_scaled)
    
    return clusters

# Main page
st.title("📊 Repository Overview")
st.markdown("---")

# Add tip box
st.info("💡 **Tip:** Use the interactive controls and dropdowns to explore different perspectives of the repository data!")

st.markdown("---")

try:
    # Load all data
    with st.spinner("Loading repository data..."):
        df_repos = load_repo_data()
        
        # Compute clusters
        df_repos['cluster'] = compute_clusters(df_repos)
        
    with st.spinner("Loading language data..."):
        df_languages = load_language_data()
    
    # Key metrics row
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Total Repos", f"{len(df_repos):,}")
    with col2:
        st.metric("Avg Stars", f"{df_repos['stars_count'].mean():,.0f}")
    with col3:
        st.metric("Total Commits", f"{df_repos['total_commits'].sum():,.0f}")
    with col4:
        st.metric("ML Clusters", "4")
    with col5:
        st.metric("Languages", f"{df_languages['primary_language'].nunique()}")
    
    st.markdown("---")
    
    # === CHART 1: Repository Age Distribution ===
    st.header("📅 Repository Age Distribution")
    
    df_repos['age_years'] = df_repos['repo_age_days'] / 365
    df_repos['age_bin'] = pd.cut(
        df_repos['age_years'], 
        bins=[0, 2, 5, 10, 15, 100],
        labels=['0-2 years', '2-5 years', '5-10 years', '10-15 years', '15+ years']
    )
    
    age_counts = df_repos['age_bin'].value_counts().sort_index()
    
    fig_age = px.bar(
        x=age_counts.index,
        y=age_counts.values,
        labels={'x': 'Repository Age', 'y': 'Number of Repositories'},
        title='',
        color=age_counts.values,
        color_continuous_scale='Blues'
    )
    fig_age.update_traces(hovertemplate='Age: %{x}<br>Count: %{y}<extra></extra>')
    fig_age.update_layout(height=400, showlegend=False)
    
    st.plotly_chart(fig_age, use_container_width=True)
    
    selected_age_bin = st.selectbox(
        "🔍 Select an age range to see repositories:",
        options=age_counts.index.tolist(),
        index=0
    )
    
    if selected_age_bin:
        repos_in_bin = df_repos[df_repos['age_bin'] == selected_age_bin][
            ['repo_name', 'stars_count', 'age_years', 'cluster', 'total_commits']
        ].sort_values('stars_count', ascending=False)
        repos_in_bin['age_years'] = repos_in_bin['age_years'].round(1)
        
        st.dataframe(
            repos_in_bin.rename(columns={
                'repo_name': 'Repository',
                'stars_count': 'Stars',
                'age_years': 'Age (years)',
                'cluster': 'ML Cluster',
                'total_commits': 'Total Commits'
            }),
            use_container_width=True,
            hide_index=True
        )
    
    st.markdown("---")
    
    # === CHART 2: Forks vs Stars Analysis ===
    st.header("🍴 Forks vs Stars Analysis")
    
    # Color toggle
    color_by = st.radio(
        "Color by:",
        options=["Language", "Age"],
        horizontal=True,
        key="fork_star_color"
    )
    
    # Merge with language data for language option
    df_fork_star = df_repos.merge(
        df_languages[['repo_full_name', 'primary_language']],
        left_on='repo_name',
        right_on='repo_full_name',
        how='left',
        suffixes=('', '_lang')
    )
    
    # Use primary_language from the merge if available, otherwise from repo data
    if 'primary_language_lang' in df_fork_star.columns:
        df_fork_star['display_language'] = df_fork_star['primary_language_lang'].fillna(df_fork_star['primary_language'])
    else:
        df_fork_star['display_language'] = df_fork_star['primary_language']
    
    # Prepare data
    df_fork_star['size_for_plot'] = df_fork_star['total_commits'].apply(lambda x: max(x, 100) if pd.notna(x) else 100)
    
    if color_by == "Language":
        # Group less common languages
        top_languages = df_fork_star['display_language'].value_counts().head(8).index
        df_fork_star['language_grouped'] = df_fork_star['display_language'].apply(
            lambda x: x if x in top_languages else 'Other'
        )
        
        fig_fork_star = px.scatter(
            df_fork_star,
            x='stars_count',
            y='forks_count',
            color='language_grouped',
            size='size_for_plot',
            hover_data={
                'repo_name': True,
                'stars_count': ':,',
                'forks_count': ':,',
                'display_language': True,
                'language_grouped': False,
                'size_for_plot': False
            },
            labels={
                'stars_count': 'Stars',
                'forks_count': 'Forks',
                'language_grouped': 'Language'
            },
            title='',
            color_discrete_sequence=px.colors.qualitative.Set3
        )
    else:  # Age
        # Create age categories with distinct colors
        df_fork_star['age_category'] = pd.cut(
            df_fork_star['age_years'],
            bins=[0, 3, 7, 12, 100],
            labels=['New (0-3y)', 'Growing (3-7y)', 'Mature (7-12y)', 'Legacy (12y+)']
        )
        
        # Use distinct color palette
        color_map = {
            'New (0-3y)': '#1f77b4',      # Blue
            'Growing (3-7y)': '#2ca02c',   # Green
            'Mature (7-12y)': '#ff7f0e',   # Orange
            'Legacy (12y+)': '#d62728'     # Red
        }
        
        fig_fork_star = px.scatter(
            df_fork_star,
            x='stars_count',
            y='forks_count',
            color='age_category',
            size='size_for_plot',
            hover_data={
                'repo_name': True,
                'stars_count': ':,',
                'forks_count': ':,',
                'age_years': ':.1f',
                'age_category': True,
                'size_for_plot': False
            },
            labels={
                'stars_count': 'Stars',
                'forks_count': 'Forks',
                'age_category': 'Age',
                'age_years': 'Age (years)'
            },
            title='',
            color_discrete_map=color_map,
            category_orders={'age_category': ['New (0-3y)', 'Growing (3-7y)', 'Mature (7-12y)', 'Legacy (12y+)']}
        )
    
    fig_fork_star.update_traces(marker=dict(line=dict(width=0.5, color='white')))
    fig_fork_star.update_layout(height=500, showlegend=True)
    
    st.plotly_chart(fig_fork_star, use_container_width=True)
    
    st.markdown("---")
    
    # === CHART 3: Language Diversity ===
    st.header("💻 Language Diversity Analysis")
    
    st.subheader("Multi-Language Projects")
    
    # Distribution of language count
    lang_count_dist = df_languages['language_count'].value_counts().sort_index()
    
    fig_lang_count = px.bar(
        x=lang_count_dist.index,
        y=lang_count_dist.values,
        labels={'x': 'Number of Languages', 'y': 'Number of Repositories'},
        title='How many languages do repositories use?',
        color=lang_count_dist.values,
        color_continuous_scale='Greens'
    )
    fig_lang_count.update_layout(height=400, showlegend=False)
    fig_lang_count.update_traces(hovertemplate='Languages: %{x}<br>Repos: %{y}<extra></extra>')
    
    st.plotly_chart(fig_lang_count, use_container_width=True)
    
    # Stats
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Avg Languages per Repo", f"{df_languages['language_count'].mean():.1f}")
    with col2:
        single_lang = len(df_languages[df_languages['language_count'] == 1])
        st.metric("Single-Language Repos", f"{single_lang}")
    with col3:
        multi_lang = len(df_languages[df_languages['language_count'] >= 5])
        st.metric("Repos with 5+ Languages", f"{multi_lang}")
    
    st.markdown("---")
    
    # === CHART 4: Language Ecosystem ===
    st.header("🌐 Primary Language Ecosystem")
    
    lang_counts = df_languages['primary_language'].value_counts().head(15)
    
    fig_lang = px.bar(
        x=lang_counts.values,
        y=lang_counts.index,
        orientation='h',
        labels={'x': 'Number of Repositories', 'y': 'Primary Language'},
        title='',
        color=lang_counts.values,
        color_continuous_scale='Viridis'
    )
    fig_lang.update_layout(height=500, showlegend=False)
    fig_lang.update_traces(hovertemplate='Language: %{y}<br>Repos: %{x}<extra></extra>')
    
    st.plotly_chart(fig_lang, use_container_width=True)
    
    selected_language = st.selectbox(
        "🔍 Select a language to see repositories:",
        options=sorted(lang_counts.index.tolist()),
        key="lang_selector"
    )
    
    if selected_language:
        lang_repos = df_languages[df_languages['primary_language'] == selected_language]
        lang_repos = lang_repos.merge(
            df_repos[['repo_name', 'stars_count', 'total_commits']],
            left_on='repo_full_name',
            right_on='repo_name',
            how='left'
        )[['repo_full_name', 'stars_count', 'language_count', 'total_commits', 'top_5_languages']].sort_values('stars_count', ascending=False).head(20)
        
        st.dataframe(
            lang_repos.rename(columns={
                'repo_full_name': 'Repository',
                'stars_count': 'Stars',
                'language_count': 'Total Languages',
                'total_commits': 'Commits',
                'top_5_languages': 'Languages Used'
            }),
            use_container_width=True,
            hide_index=True
        )

except Exception as e:
    st.error(f"Error loading data: {str(e)}")
    st.info("Make sure all required BigQuery tables exist and have data.")