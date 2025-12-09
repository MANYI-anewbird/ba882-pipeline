import streamlit as st
from google.cloud import bigquery
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

# Page config
st.set_page_config(page_title="Repository Deep Dive", page_icon="🔍", layout="wide")

# Connect to BigQuery
@st.cache_resource
def get_bigquery_client():
    return bigquery.Client(project="ba-882-fall25-team8")

client = get_bigquery_client()

# Load functions
@st.cache_data(ttl=3600)
def load_repo_list():
    """Get list of all repositories with basic info"""
    query = """
    SELECT 
        repo_name,
        stars_count,
        forks_count,
        total_commits,
        total_contributors,
        repo_age_days,
        stars_per_day,
        language_count
    FROM `ba-882-fall25-team8.cleaned_github_data.repo_ml_ready`
    WHERE stars_count IS NOT NULL
    ORDER BY stars_count DESC
    """
    df = client.query(query).to_dataframe()
    
    # Compute clusters
    feature_cols = ['stars_count', 'forks_count', 'total_commits', 'total_contributors', 
                    'repo_age_days', 'stars_per_day', 'language_count']
    available_cols = [c for c in feature_cols if c in df.columns]
    X = df[available_cols].fillna(0)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
    df['cluster'] = kmeans.fit_predict(X_scaled)
    
    return df

@st.cache_data(ttl=3600)
def load_repo_details(repo_name):
    """Load detailed metrics for a specific repo"""
    query = f"""
    SELECT *
    FROM `ba-882-fall25-team8.cleaned_github_data.repo_ml_ready`
    WHERE repo_name = '{repo_name}'
    LIMIT 1
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_readme_data(repo_name):
    """Load README analysis for a repo"""
    query = f"""
    SELECT 
        repo_full_name,
        readme_filename,
        content_length,
        word_count_estimate,
        line_count,
        header_count,
        code_block_count,
        link_count,
        image_count,
        has_installation,
        has_usage,
        has_contributing,
        has_license,
        has_tests,
        has_documentation,
        has_badges,
        content_preview
    FROM `ba-882-fall25-team8.cleaned_github_data.readme_summary`
    WHERE repo_full_name = '{repo_name}'
    LIMIT 1
    """
    result = client.query(query).to_dataframe()
    return result if len(result) > 0 else None

@st.cache_data(ttl=3600)
def load_commit_timeline(repo_name):
    """Load commit history for timeline"""
    query = f"""
    SELECT 
        commit_date_only as commit_date,
        COUNT(*) as commit_count,
        COUNT(DISTINCT author_name) as unique_authors
    FROM `ba-882-fall25-team8.cleaned_github_data.commits_clean`
    WHERE repo_full_name = '{repo_name}'
    AND commit_date_only IS NOT NULL
    GROUP BY commit_date
    ORDER BY commit_date DESC
    LIMIT 365
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_top_contributors(repo_name):
    """Load top contributors for a repo"""
    query = f"""
    SELECT 
        author_name,
        COUNT(*) as commit_count,
        MIN(commit_date_only) as first_commit,
        MAX(commit_date_only) as last_commit
    FROM `ba-882-fall25-team8.cleaned_github_data.commits_clean`
    WHERE repo_full_name = '{repo_name}'
    AND author_name IS NOT NULL
    GROUP BY author_name
    ORDER BY commit_count DESC
    LIMIT 20
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_language_breakdown(repo_name):
    """Load language composition"""
    query = f"""
    SELECT 
        primary_language,
        primary_language_pct,
        secondary_language,
        secondary_language_pct,
        tertiary_language,
        tertiary_language_pct,
        language_count,
        top_5_languages
    FROM `ba-882-fall25-team8.cleaned_github_data.language_summary`
    WHERE repo_full_name = '{repo_name}'
    LIMIT 1
    """
    result = client.query(query).to_dataframe()
    return result if len(result) > 0 else None

@st.cache_data(ttl=3600)
def load_llm_enrichment(repo_name):
    """Load LLM-enriched data for a repo"""
    query = f"""
    SELECT 
        category,
        tech_stack,
        complexity_level,
        complexity_score,
        audience,
        use_cases
    FROM `ba-882-fall25-team8.cleaned_github_data.repo_llm_enriched`
    WHERE repo_name = '{repo_name}'
    LIMIT 1
    """
    result = client.query(query).to_dataframe()
    return result if len(result) > 0 else None

# Main page
st.title("🔍 Repository Deep Dive")
st.markdown("---")

# Add tip box
st.info("💡 **Tip:** Select a repository from the dropdown to explore its detailed metrics, documentation quality, and activity patterns!")

st.markdown("---")

try:
    # Load repo list
    df_repos = load_repo_list()
    
    # Repo selector
    col1, col2 = st.columns([3, 1])
    with col1:
        selected_repo = st.selectbox(
            "🎯 Select Repository:",
            options=df_repos['repo_name'].tolist(),
            index=0
        )
    
    with col2:
        st.metric("Total Repos Available", f"{len(df_repos):,}")
    
    if selected_repo:
        st.markdown("---")
        
        # Load all data
        repo_details = load_repo_details(selected_repo)
        readme_data = load_readme_data(selected_repo)
        commit_timeline = load_commit_timeline(selected_repo)
        contributors = load_top_contributors(selected_repo)
        languages = load_language_breakdown(selected_repo)
        llm_data = load_llm_enrichment(selected_repo)
        
        # Get cluster for this repo
        repo_cluster = df_repos[df_repos['repo_name'] == selected_repo]['cluster'].values[0]
        
        # Key Metrics
        st.header(f"📊 {selected_repo}")
        
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        
        with col1:
            stars = int(repo_details['stars_count'].values[0]) if len(repo_details) > 0 and pd.notna(repo_details['stars_count'].values[0]) else 0
            st.metric("⭐ Stars", f"{stars:,}")
        with col2:
            forks = int(repo_details['forks_count'].values[0]) if len(repo_details) > 0 and pd.notna(repo_details['forks_count'].values[0]) else 0
            st.metric("🍴 Forks", f"{forks:,}")
        with col3:
            age_days = int(repo_details['repo_age_days'].values[0]) if len(repo_details) > 0 and pd.notna(repo_details['repo_age_days'].values[0]) else 0
            st.metric("📅 Age", f"{age_days // 365} years")
        with col4:
            commits = int(repo_details['total_commits'].values[0]) if len(repo_details) > 0 and pd.notna(repo_details['total_commits'].values[0]) else 0
            st.metric("💻 Commits", f"{commits:,}")
        with col5:
            contribs = int(repo_details['total_contributors'].values[0]) if len(repo_details) > 0 and pd.notna(repo_details['total_contributors'].values[0]) else 0
            st.metric("👥 Contributors", f"{contribs:,}")
        with col6:
            st.metric("🤖 ML Cluster", f"Cluster {repo_cluster}")
        
        st.markdown("---")
        
        # === LLM Enrichment Section (NEW - Phase 3) ===
        # === LLM Enrichment Section (NEW - Phase 3) ===
        st.header("🧠 AI-Powered Insights")
        
        if llm_data is not None and len(llm_data) > 0:
            llm = llm_data.iloc[0]
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                category = str(llm['category']) if pd.notna(llm['category']) else "Not classified"
                st.metric("📁 Category", category)
            
            with col2:
                complexity = str(llm['complexity_level']) if pd.notna(llm['complexity_level']) else "Unknown"
                complexity_score = int(llm['complexity_score']) if pd.notna(llm['complexity_score']) else 0
                st.metric("📈 Complexity", f"{complexity} ({complexity_score}/3)")
            
            with col3:
                audience_raw = llm['audience']
                if pd.isna(audience_raw).any() if hasattr(audience_raw, '__iter__') and not isinstance(audience_raw, str) else pd.isna(audience_raw):
                    audience = "General"
                elif isinstance(audience_raw, (list, np.ndarray)):
                    audience = ", ".join(str(x) for x in audience_raw[:3])
                else:
                    audience = str(audience_raw).strip('[]').replace("'", "")[:50]
                st.metric("👥 Target Audience", audience)
            
            # Use cases
            use_cases_raw = llm['use_cases']
            if isinstance(use_cases_raw, (list, np.ndarray)):
                use_cases = ", ".join(str(x) for x in use_cases_raw)
            elif pd.notna(use_cases_raw):
                use_cases = str(use_cases_raw).strip('[]').replace("'", "")
            else:
                use_cases = "Not specified"
            st.info(f"**🎯 Use Cases:** {use_cases}")
            
            # Tech stack
            tech_stack_raw = llm['tech_stack']
            if isinstance(tech_stack_raw, (list, np.ndarray)):
                tech_stack = ", ".join(str(x) for x in tech_stack_raw)
            elif pd.notna(tech_stack_raw):
                tech_stack = str(tech_stack_raw).strip('[]').replace("'", "")
            else:
                tech_stack = "Not specified"
            st.info(f"**🔧 Tech Stack:** {tech_stack}")
        else:
            st.warning("⚠️ No AI-enriched data available for this repository")
        st.markdown("---")
        
        # README Analysis
        st.header("📄 README Documentation Analysis")
        
        if readme_data is not None and len(readme_data) > 0:
            readme = readme_data.iloc[0]
            
            # README Metrics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                length = int(readme['content_length']) if pd.notna(readme['content_length']) else 0
                st.metric("📝 Content Length", f"{length:,} chars")
            with col2:
                words = int(readme['word_count_estimate']) if pd.notna(readme['word_count_estimate']) else 0
                st.metric("🔤 Word Count", f"{words:,}")
            with col3:
                headers = int(readme['header_count']) if pd.notna(readme['header_count']) else 0
                st.metric("📑 Headers", f"{headers}")
            with col4:
                code_blocks = int(readme['code_block_count']) if pd.notna(readme['code_block_count']) else 0
                st.metric("💻 Code Blocks", f"{code_blocks}")
            
            st.markdown("---")
            
            # Completeness Radar
            st.subheader("📊 Documentation Completeness")
            
            completeness_data = {
                'Section': ['Installation', 'Usage', 'Contributing', 'License', 'Tests', 'Documentation'],
                'Present': [
                    1 if readme['has_installation'] else 0,
                    1 if readme['has_usage'] else 0,
                    1 if readme['has_contributing'] else 0,
                    1 if readme['has_license'] else 0,
                    1 if readme['has_tests'] else 0,
                    1 if readme['has_documentation'] else 0
                ]
            }
            
            df_completeness = pd.DataFrame(completeness_data)
            
            fig_radar = go.Figure()
            fig_radar.add_trace(go.Scatterpolar(
                r=df_completeness['Present'],
                theta=df_completeness['Section'],
                fill='toself',
                name='Completeness',
                line_color='rgb(0, 123, 255)'
            ))
            
            fig_radar.update_layout(
                polar=dict(
                    radialaxis=dict(
                        visible=True,
                        range=[0, 1],
                        ticktext=['Missing', 'Present'],
                        tickvals=[0, 1]
                    )
                ),
                showlegend=False,
                height=400
            )
            
            st.plotly_chart(fig_radar, use_container_width=True)
            
            # Completeness summary
            sections_present = sum(df_completeness['Present'])
            total_sections = len(df_completeness)
            completeness_pct = (sections_present / total_sections) * 100
            
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                st.metric(
                    "📋 Documentation Completeness",
                    f"{sections_present} / {total_sections} sections",
                    delta=f"{completeness_pct:.0f}%"
                )
            
            missing_sections = [section for section, present in zip(df_completeness['Section'], df_completeness['Present']) if present == 0]
            if missing_sections:
                st.warning(f"⚠️ Missing sections: {', '.join(missing_sections)}")
            else:
                st.success("✅ All key documentation sections present!")
            
            # README Preview
            st.subheader("👀 README Preview")
            if pd.notna(readme['content_preview']):
                preview_text = str(readme['content_preview'])[:500]
                st.text_area(
                    "First 500 characters:",
                    preview_text,
                    height=150,
                    disabled=True
                )
            else:
                st.info("No preview available")
        else:
            st.warning("⚠️ No README data available for this repository")
        
        st.markdown("---")
        
        # Commit Timeline
        st.header("📈 Commit Activity Timeline")
        
        if len(commit_timeline) > 0:
            commit_timeline['commit_date'] = pd.to_datetime(commit_timeline['commit_date'])
            commit_timeline = commit_timeline.sort_values('commit_date')
            
            fig_timeline = px.line(
                commit_timeline,
                x='commit_date',
                y='commit_count',
                labels={'commit_date': 'Date', 'commit_count': 'Commits'},
                title=f'Daily Commit Activity (Last {len(commit_timeline)} days with data)'
            )
            fig_timeline.update_traces(line_color='rgb(0, 123, 255)', line_width=2)
            fig_timeline.update_layout(height=400, hovermode='x unified')
            
            st.plotly_chart(fig_timeline, use_container_width=True)
            
            col1, col2, col3 = st.columns(3)
            with col1:
                total = commit_timeline['commit_count'].sum()
                st.metric("Total Commits", f"{int(total):,}")
            with col2:
                avg = commit_timeline['commit_count'].mean()
                st.metric("Avg Commits/Day", f"{avg:.1f}")
            with col3:
                peak = commit_timeline['commit_count'].max()
                st.metric("Peak Activity", f"{int(peak)} commits")
        else:
            st.info("No commit timeline data available")
        
        st.markdown("---")
        
        # Top Contributors
        st.header("👥 Top Contributors")
        
        if len(contributors) > 0:
            top_10 = contributors.head(10)
            
            fig_contributors = px.bar(
                top_10,
                x='commit_count',
                y='author_name',
                orientation='h',
                labels={'commit_count': 'Commits', 'author_name': 'Contributor'},
                title='Top 10 Contributors by Commit Count',
                color='commit_count',
                color_continuous_scale='Blues'
            )
            fig_contributors.update_layout(height=400, showlegend=False)
            
            st.plotly_chart(fig_contributors, use_container_width=True)
            
            st.subheader("📋 All Contributors")
            contributors_display = contributors.copy()
            contributors_display['first_commit'] = pd.to_datetime(contributors_display['first_commit']).dt.strftime('%Y-%m-%d')
            contributors_display['last_commit'] = pd.to_datetime(contributors_display['last_commit']).dt.strftime('%Y-%m-%d')
            
            st.dataframe(
                contributors_display.rename(columns={
                    'author_name': 'Contributor',
                    'commit_count': 'Commits',
                    'first_commit': 'First Commit',
                    'last_commit': 'Last Commit'
                }),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No contributor data available")
        
        st.markdown("---")
        
        # Language Breakdown
        st.header("💻 Language Composition")
        
        if languages is not None and len(languages) > 0:
            lang = languages.iloc[0]
            
            lang_data = []
            if pd.notna(lang['primary_language']):
                lang_data.append({
                    'Language': str(lang['primary_language']),
                    'Percentage': float(lang['primary_language_pct']) if pd.notna(lang['primary_language_pct']) else 0
                })
            if pd.notna(lang['secondary_language']):
                lang_data.append({
                    'Language': str(lang['secondary_language']),
                    'Percentage': float(lang['secondary_language_pct']) if pd.notna(lang['secondary_language_pct']) else 0
                })
            if pd.notna(lang['tertiary_language']):
                lang_data.append({
                    'Language': str(lang['tertiary_language']),
                    'Percentage': float(lang['tertiary_language_pct']) if pd.notna(lang['tertiary_language_pct']) else 0
                })
            
            if len(lang_data) > 0:
                df_lang = pd.DataFrame(lang_data)
                
                fig_lang = px.bar(
                    df_lang,
                    x='Percentage',
                    y='Language',
                    orientation='h',
                    labels={'Percentage': 'Percentage of Codebase', 'Language': ''},
                    title='Language Distribution',
                    color='Percentage',
                    color_continuous_scale='Viridis'
                )
                fig_lang.update_layout(height=300, showlegend=False)
                
                st.plotly_chart(fig_lang, use_container_width=True)
                
                col1, col2 = st.columns(2)
                with col1:
                    lang_count = int(lang['language_count']) if pd.notna(lang['language_count']) else 0
                    st.metric("Total Languages", lang_count)
                with col2:
                    if pd.notna(lang['top_5_languages']):
                        st.info(f"**Top 5:** {lang['top_5_languages']}")
            else:
                st.info("No language breakdown data available")
        else:
            st.warning("⚠️ No language data available for this repository")

except Exception as e:
    st.error(f"Error loading data: {str(e)}")
    st.info("Make sure all required BigQuery tables exist and have data.")