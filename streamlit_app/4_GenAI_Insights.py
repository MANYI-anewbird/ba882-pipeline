import streamlit as st
from google.cloud import bigquery
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np

# Page config
st.set_page_config(page_title="GenAI Insights", page_icon="🧠", layout="wide")

# Connect to BigQuery
@st.cache_resource
def get_bigquery_client():
    return bigquery.Client(project="ba-882-fall25-team8")

client = get_bigquery_client()

# Load data
@st.cache_data(ttl=3600)
def load_llm_enriched_data():
    """Load LLM-enriched repo data"""
    query = """
    SELECT 
        r.repo_name,
        r.stars_count,
        r.forks_count,
        r.total_commits,
        r.total_contributors,
        r.repo_age_days,
        r.primary_language,
        r.category,
        r.tech_stack,
        r.complexity_level,
        r.complexity_score,
        r.audience,
        r.use_cases
    FROM `ba-882-fall25-team8.cleaned_github_data.repo_ml_ready` r
    WHERE r.stars_count IS NOT NULL
    ORDER BY r.stars_count DESC
    """
    return client.query(query).to_dataframe()

def parse_array_string(s):
    """Parse string like ['item1' 'item2'] into list - handles space-separated items"""
    if pd.isna(s) or s == 'None' or s == 'nan' or s is None or s == '[]':
        return []
    if isinstance(s, (list, np.ndarray)):
        return list(s)
    
    # Convert to string and clean
    s = str(s).strip()
    
    # Remove outer brackets
    if s.startswith('[') and s.endswith(']'):
        s = s[1:-1]
    
    # Remove quotes and split by spaces or common delimiters
    # Handle format: 'item1' 'item2' or "item1" "item2"
    items = []
    s = s.replace('"', "'")  # Normalize quotes
    
    # Split by ' ' (quote-space-quote pattern)
    parts = s.split("' '")
    for part in parts:
        cleaned = part.strip().strip("'").strip('"').strip()
        if cleaned and cleaned != 'None':
            items.append(cleaned)
    
    return items

# Main page
st.title("🧠 GenAI Insights")
st.markdown("---")

st.info("💡 **Tip:** Explore AI-powered analysis of GitHub repositories! Our LLM analyzed README files to extract categories, complexity levels, target audiences, and use cases.")

st.markdown("---")

try:
    with st.spinner("Loading AI-enriched data..."):
        df = load_llm_enriched_data()
    
    llm_coverage = df['category'].notna().sum()
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("📦 Total Repos", f"{len(df):,}")
    with col2:
        st.metric("🧠 AI-Analyzed", f"{llm_coverage:,}")
    with col3:
        coverage_pct = (llm_coverage / len(df)) * 100 if len(df) > 0 else 0
        st.metric("📊 Coverage", f"{coverage_pct:.1f}%")
    with col4:
        categories = df['category'].dropna().nunique()
        st.metric("📁 Categories", f"{categories}")
    
    st.markdown("---")
    
    # === SECTION 1: Category Distribution + Sankey to Use Cases ===
    st.header("📁 Repository Categories & Use Cases Flow")
    
    # Pie chart (full width)
    category_counts = df['category'].value_counts().reset_index()
    category_counts.columns = ['Category', 'Count']
    
    fig_cat_pie = px.pie(
        category_counts,
        values='Count',
        names='Category',
        title='Repository Distribution by Category',
        color_discrete_sequence=px.colors.qualitative.Set3,
        hole=0.4
    )
    fig_cat_pie.update_traces(textposition='inside', textinfo='percent+label')
    fig_cat_pie.update_layout(height=450)
    st.plotly_chart(fig_cat_pie, use_container_width=True)
    
    st.markdown("---")
    
    # Sankey Diagram: Category → Use Cases
    # Sankey Diagram: Category → Use Cases
    st.subheader("🔀 Category to Use Cases Flow")
    st.caption("Note: 'cli-automation' is excluded as it appears in 90%+ of repos")
    
    # Build Sankey data - exclude cli-automation (too common)
    sankey_data = []
    for _, row in df[df['category'].notna() & df['use_cases'].notna()].iterrows():
        category = row['category']
        use_cases = parse_array_string(row['use_cases'])
        for uc in use_cases:
            if uc and uc != 'cli-automation':  # Exclude cli-automation
                sankey_data.append({'category': category, 'use_case': uc})
    
    sankey_df = pd.DataFrame(sankey_data)
    
    if len(sankey_df) > 0:
        # Aggregate counts
        sankey_agg = sankey_df.groupby(['category', 'use_case']).size().reset_index(name='count')
        
        # Filter to top use cases to avoid clutter
        top_use_cases = sankey_df['use_case'].value_counts().head(8).index.tolist()
        sankey_agg = sankey_agg[sankey_agg['use_case'].isin(top_use_cases)]
        
        # Filter out small connections (less than 2)
        sankey_agg = sankey_agg[sankey_agg['count'] >= 2]
        
        # Create node lists
        categories = sorted(sankey_agg['category'].unique().tolist())
        use_cases = sankey_agg.groupby('use_case')['count'].sum().sort_values(ascending=False).index.tolist()
        all_nodes = categories + use_cases
        
        # Create index mapping
        node_indices = {node: idx for idx, node in enumerate(all_nodes)}
        
        # Build source, target, value lists
        sources = [node_indices[cat] for cat in sankey_agg['category']]
        targets = [node_indices[uc] for uc in sankey_agg['use_case']]
        values = sankey_agg['count'].tolist()
        
        # Define colors directly (RGBA format)
        category_colors = [
            'rgba(102, 194, 165, 1)',   # teal
            'rgba(252, 141, 98, 1)',    # orange
            'rgba(141, 160, 203, 1)',   # purple
            'rgba(231, 138, 195, 1)',   # pink
            'rgba(166, 216, 84, 1)',    # green
            'rgba(255, 217, 47, 1)',    # yellow
            'rgba(229, 196, 148, 1)',   # tan
            'rgba(179, 179, 179, 1)',   # gray
            'rgba(147, 112, 219, 1)',   # medium purple
            'rgba(60, 179, 113, 1)',    # medium sea green
            'rgba(255, 165, 0, 1)',     # orange2
            'rgba(106, 90, 205, 1)',    # slate blue
            'rgba(240, 128, 128, 1)',   # light coral
            'rgba(32, 178, 170, 1)',    # light sea green
        ]
        
        # Assign colors to categories
        cat_color_map = {cat: category_colors[i % len(category_colors)] for i, cat in enumerate(categories)}
        
        # Node colors
        node_colors = [cat_color_map[cat] for cat in categories] + ['rgba(200, 200, 200, 1)'] * len(use_cases)
        
        # Link colors (based on source category, with transparency)
        link_colors = [cat_color_map[sankey_agg.iloc[i]['category']].replace(', 1)', ', 0.4)') for i in range(len(sankey_agg))]
        
        fig_sankey = go.Figure(data=[go.Sankey(
            node=dict(
                pad=20,
                thickness=25,
                line=dict(color="black", width=0.5),
                label=all_nodes,
                color=node_colors
            ),
            link=dict(
                source=sources,
                target=targets,
                value=values,
                color=link_colors
            )
        )])
        
        fig_sankey.update_layout(
            title_text="How Categories Map to Use Cases",
            font_size=11,
            height=550
        )
        st.plotly_chart(fig_sankey, use_container_width=True)
    
    # Category drill-down table
    st.subheader("🔍 Explore by Category")
    available_categories = sorted(df['category'].dropna().unique())
    if available_categories:
        selected_category = st.selectbox("Select a category:", options=available_categories)
        
        if selected_category:
            cat_repos = df[df['category'] == selected_category].sort_values('stars_count', ascending=False)[
                ['repo_name', 'stars_count', 'forks_count', 'complexity_level', 'primary_language']
            ].copy()
            
            st.markdown(f"**Found {len(cat_repos)} repositories in {selected_category}**")
            
            st.dataframe(
                cat_repos.rename(columns={
                    'repo_name': 'Repository',
                    'stars_count': 'Stars',
                    'forks_count': 'Forks',
                    'complexity_level': 'Complexity',
                    'primary_language': 'Language'
                }),
                use_container_width=True,
                hide_index=True,
                height=400
            )
    
    st.markdown("---")
    
    # === SECTION 2: Complexity Analysis (Enhanced) ===
    st.header("📈 Complexity Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Complexity level distribution
        complexity_counts = df['complexity_level'].value_counts().reset_index()
        complexity_counts.columns = ['Complexity', 'Count']
        
        complexity_order = ['beginner', 'intermediate', 'advanced']
        complexity_counts['Complexity'] = pd.Categorical(
            complexity_counts['Complexity'], 
            categories=complexity_order, 
            ordered=True
        )
        complexity_counts = complexity_counts.sort_values('Complexity').dropna()
        
        color_map = {'beginner': '#2ecc71', 'intermediate': '#f39c12', 'advanced': '#e74c3c'}
        
        fig_complexity = px.bar(
            complexity_counts,
            x='Complexity',
            y='Count',
            title='Repository Complexity Distribution',
            color='Complexity',
            color_discrete_map=color_map
        )
        fig_complexity.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig_complexity, use_container_width=True)
    
    with col2:
        # Toggle between Stars and Forks
        metric_choice = st.radio("Show by:", options=["Stars", "Forks"], horizontal=True, key="complexity_metric")
        
        metric_col = 'stars_count' if metric_choice == "Stars" else 'forks_count'
        
        # Calculate averages for grouped bar chart
        df_complexity = df[df['complexity_level'].notna()].copy()
        complexity_avg = df_complexity.groupby('complexity_level')[metric_col].mean().reset_index()
        complexity_avg.columns = ['Complexity', f'Avg {metric_choice}']
        
        complexity_avg['Complexity'] = pd.Categorical(
            complexity_avg['Complexity'],
            categories=complexity_order,
            ordered=True
        )
        complexity_avg = complexity_avg.sort_values('Complexity')
        
        fig_metric = px.bar(
            complexity_avg,
            x='Complexity',
            y=f'Avg {metric_choice}',
            title=f'Average {metric_choice} by Complexity Level',
            color='Complexity',
            color_discrete_map=color_map
        )
        fig_metric.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig_metric, use_container_width=True)
    
    # Complexity stats
    col1, col2, col3 = st.columns(3)
    
    beginner_repos = df[df['complexity_level'] == 'beginner']
    intermediate_repos = df[df['complexity_level'] == 'intermediate']
    advanced_repos = df[df['complexity_level'] == 'advanced']
    
    with col1:
        avg_stars = beginner_repos['stars_count'].mean() if len(beginner_repos) > 0 else 0
        st.metric("🟢 Beginner", f"{len(beginner_repos)} repos", delta=f"Avg {avg_stars:,.0f} ⭐")
    with col2:
        avg_stars = intermediate_repos['stars_count'].mean() if len(intermediate_repos) > 0 else 0
        st.metric("🟡 Intermediate", f"{len(intermediate_repos)} repos", delta=f"Avg {avg_stars:,.0f} ⭐")
    with col3:
        avg_stars = advanced_repos['stars_count'].mean() if len(advanced_repos) > 0 else 0
        st.metric("🔴 Advanced", f"{len(advanced_repos)} repos", delta=f"Avg {avg_stars:,.0f} ⭐")
    
    st.markdown("---")

    # === SECTION: Language & LLM Insights ===
    st.header("🌐 Language & AI Classification Insights")
    
    # --- Chart 1: Language ↔ Category Heatmap ---
    st.subheader("📊 Language vs Category Heatmap")
    
    # Prepare data - use primary_language
    lang_cat_df = df[df['primary_language'].notna() & df['category'].notna()].copy()
    
    # Get top 10 languages by count
    top_languages = lang_cat_df['primary_language'].value_counts().head(10).index.tolist()
    lang_cat_filtered = lang_cat_df[lang_cat_df['primary_language'].isin(top_languages)]
    
    # Create pivot table
    lang_cat_pivot = pd.crosstab(lang_cat_filtered['primary_language'], lang_cat_filtered['category'])
    
    # Sort by total repos per language
    lang_cat_pivot['_total'] = lang_cat_pivot.sum(axis=1)
    lang_cat_pivot = lang_cat_pivot.sort_values('_total', ascending=False).drop('_total', axis=1)
    
    fig_heatmap = px.imshow(
        lang_cat_pivot,
        labels=dict(x="Category", y="Language", color="Repos"),
        title="Which Languages Dominate Which Categories",
        color_continuous_scale="Blues",
        aspect="auto"
    )
    fig_heatmap.update_layout(height=450)
    st.plotly_chart(fig_heatmap, use_container_width=True)
    
    st.markdown("---")
    
    # --- Chart 2: Language Complexity Profile (Grouped Bar) ---
    st.subheader("📈 Language Complexity Profile")
    
    lang_complexity_df = df[df['primary_language'].notna() & df['complexity_level'].notna()].copy()
    lang_complexity_filtered = lang_complexity_df[lang_complexity_df['primary_language'].isin(top_languages)]
    
    # Create counts
    lang_complexity_counts = lang_complexity_filtered.groupby(['primary_language', 'complexity_level']).size().reset_index(name='count')
    
    # Ensure proper ordering
    complexity_order = ['beginner', 'intermediate', 'advanced']
    lang_complexity_counts['complexity_level'] = pd.Categorical(
        lang_complexity_counts['complexity_level'],
        categories=complexity_order,
        ordered=True
    )
    
    # Sort languages by total count
    lang_order = lang_complexity_filtered['primary_language'].value_counts().index.tolist()
    lang_complexity_counts['primary_language'] = pd.Categorical(
        lang_complexity_counts['primary_language'],
        categories=lang_order,
        ordered=True
    )
    
    color_map = {'beginner': '#2ecc71', 'intermediate': '#f39c12', 'advanced': '#e74c3c'}
    
    fig_lang_complexity = px.bar(
        lang_complexity_counts.sort_values(['primary_language', 'complexity_level']),
        x='primary_language',
        y='count',
        color='complexity_level',
        barmode='group',
        title='Complexity Distribution by Language (Top 10 Languages)',
        color_discrete_map=color_map,
        category_orders={'complexity_level': complexity_order}
    )
    fig_lang_complexity.update_layout(
        height=450,
        xaxis_title="Language",
        yaxis_title="Number of Repos",
        legend_title="Complexity"
    )
    st.plotly_chart(fig_lang_complexity, use_container_width=True)
    
    st.markdown("---")
    
    # --- Chart 3: Sunburst (Category → Language → Complexity) ---
    st.subheader("🌞 Category → Language → Complexity Breakdown")
    st.caption("Click on segments to drill down")
    
    sunburst_df = df[
        df['category'].notna() & 
        df['primary_language'].notna() & 
        df['complexity_level'].notna()
    ].copy()
    
    # Filter to top languages to avoid clutter
    top_langs_sunburst = sunburst_df['primary_language'].value_counts().head(8).index.tolist()
    sunburst_df = sunburst_df[sunburst_df['primary_language'].isin(top_langs_sunburst)]
    
    # Aggregate
    sunburst_agg = sunburst_df.groupby(['category', 'primary_language', 'complexity_level']).size().reset_index(name='count')
    
    fig_sunburst = px.sunburst(
        sunburst_agg,
        path=['category', 'primary_language', 'complexity_level'],
        values='count',
        title='Hierarchical View: Category → Language → Complexity',
        color='count',
        color_continuous_scale='Viridis'
    )
    fig_sunburst.update_layout(height=600)
    fig_sunburst.update_traces(textinfo='label+percent parent')
    st.plotly_chart(fig_sunburst, use_container_width=True)
    
    st.markdown("---") 


    # === SECTION 3: Target Audience Analysis ===
    st.header("👥 Target Audience Analysis")
    
    # Parse audience and count individual values
    audience_counts = {}
    for audiences in df['audience'].dropna():
        parsed = parse_array_string(audiences)
        for audience in parsed:
            if audience:
                audience_counts[audience] = audience_counts.get(audience, 0) + 1
    
    if audience_counts:
        audience_df = pd.DataFrame(list(audience_counts.items()), columns=['Audience', 'Count'])
        audience_df = audience_df.sort_values('Count', ascending=False)
        
        fig_audience = px.bar(
            audience_df.head(12),
            x='Count',
            y='Audience',
            orientation='h',
            title='Top Target Audiences',
            color='Count',
            color_continuous_scale='Purples'
        )
        fig_audience.update_layout(height=450, showlegend=False)
        st.plotly_chart(fig_audience, use_container_width=True)
        
        # Filter by audience table
        st.subheader("🔍 Filter Repositories by Audience")
        
        selected_audience = st.selectbox(
            "Select an audience type:",
            options=audience_df['Audience'].tolist(),
            key="audience_filter"
        )
        
        if selected_audience:
            # Filter repos that contain this audience
            filtered_repos = []
            for _, row in df.iterrows():
                audiences = parse_array_string(row['audience'])
                if selected_audience in audiences:
                    filtered_repos.append(row)
            
            if filtered_repos:
                filtered_df = pd.DataFrame(filtered_repos)
                display_df = filtered_df.sort_values('stars_count', ascending=False)[
                    ['repo_name', 'category', 'stars_count', 'forks_count', 'complexity_level']
                ].copy()
                
                st.markdown(f"**Found {len(display_df)} repositories targeting {selected_audience}**")
                
                st.dataframe(
                    display_df.rename(columns={
                        'repo_name': 'Repository',
                        'category': 'Category',
                        'stars_count': 'Stars',
                        'forks_count': 'Forks',
                        'complexity_level': 'Complexity'
                    }),
                    use_container_width=True,
                    hide_index=True,
                    height=400
                )
    else:
        st.warning("No audience data available")
    
    st.markdown("---")
    
    # === SECTION 4: Search & Filter ===
    st.header("🔍 Repository Search")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        filter_category = st.multiselect(
            "Filter by Category:",
            options=sorted(df['category'].dropna().unique()),
            default=[]
        )
    
    with col2:
        filter_complexity = st.multiselect(
            "Filter by Complexity:",
            options=['beginner', 'intermediate', 'advanced'],
            default=[]
        )
    
    with col3:
        min_stars = st.slider(
            "Minimum Stars:",
            min_value=0,
            max_value=int(df['stars_count'].max()),
            value=0,
            step=1000
        )
    
    filtered_df = df.copy()
    
    if filter_category:
        filtered_df = filtered_df[filtered_df['category'].isin(filter_category)]
    
    if filter_complexity:
        filtered_df = filtered_df[filtered_df['complexity_level'].isin(filter_complexity)]
    
    filtered_df = filtered_df[filtered_df['stars_count'] >= min_stars]
    
    st.markdown(f"**Found {len(filtered_df)} repositories matching your criteria**")
    
    st.dataframe(
        filtered_df[['repo_name', 'category', 'complexity_level', 'stars_count', 'forks_count', 'primary_language']].head(50).rename(columns={
            'repo_name': 'Repository',
            'category': 'Category',
            'complexity_level': 'Complexity',
            'stars_count': 'Stars',
            'forks_count': 'Forks',
            'primary_language': 'Language'
        }),
        use_container_width=True,
        hide_index=True,
        height=400
    )

except Exception as e:
    st.error(f"Error loading data: {str(e)}")
    st.info("Make sure all required BigQuery tables exist and have data.")