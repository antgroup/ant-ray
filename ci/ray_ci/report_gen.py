import os
import shutil
from pathlib import Path
from textwrap import dedent
import re

BASE_DIR = os.getenv("BAZEL_DIR")
COMMIT_HASH = os.getenv("COMMIT_HASH")
REPORT_DIR = Path(f"{BASE_DIR}/reports/")
LOG_SOURCE_DIR = Path(f"{BASE_DIR}/artifacts/bazel_event_logs/")
FAILED_LOGS_DIR = Path(f"{BASE_DIR}/artifacts/failed_test_logs/")

MODERN_CSS = """
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
<style>
    :root {
        --bs-body-bg: #0f172a;
        --bs-body-color: #e2e8f0;
        --bs-primary: #3b82f6;
        --bs-border-color: #334155;
    }
    body { 
        background-color: var(--bs-body-bg);
        color: var(--bs-body-color);
    }
    .card {
        background-color: #1e293b;
        border: 1px solid var(--bs-border-color);
        box-shadow: 0 1px 3px rgba(0,0,0,0.2);
    }
    .list-group-item {
        background-color: #1e293b;
        color: #e2e8f0;
        border-color: var(--bs-border-color);
    }
    /* Fixed navigation styling */
    .nav-sidebar {
        width: 280px;
        position: fixed;
        left: 0;
        top: 0;
        bottom: 0;
        overflow-y: auto;
        padding: 1rem;
        background-color: #1e293b;
        border-right: 1px solid var(--bs-border-color);
        padding-right: 0.5rem;
        z-index: 1000;
    }
    .main-content {
        margin-left: 280px;
        padding: 2rem;
        min-height: 100vh;
        overflow-y: auto;
        height: 100vh;
    }
    /* Search bar styling */
    .search-container {
        position: sticky;
        top: 0;
        z-index: 1020;
        background-color: var(--bs-body-bg);
        padding: 1rem 0;
        margin: -1rem -0.5rem 1rem;
    }
    
    /* Search input styling */
    .search-container input {
        background-color: #1e293b;
        border-color: var(--bs-border-color);
        color: white;
    }
    
    .search-container input::placeholder {
        color: rgba(255, 255, 255, 0.6);
    }
    /* Active state styling */
    .list-group-item.active {
        background-color: #1e40af !important;
        border-color: #3b82f6 !important;
        color: white !important;
        border-left: 4px solid var(--bs-primary);
    }
    /* Improved code blocks */
    pre {
        background-color: #0f172a;
        color: #7dd3fc;
        padding: 1rem;
        border-radius: 0.375rem;
        border: 1px solid var(--bs-border-color);
        overflow-x: auto;
    }
    /* Table styling */
    .table {
        color: #e2e8f0;
    }
    .table td {
        border-color: var(--bs-border-color);
    }
    /* Updated text colors */
    .text-primary {
        color: white !important;
    }
    .text-muted {
        color: #94a3b8 !important;
    }
    
    /* Scrollable log list */
    .log-list {
        max-height: 400px;
        overflow-y: auto;
        background-color: #1e293b;
        border-radius: 0.375rem;
        border: 1px solid var(--bs-border-color);
    }
    .log-list::-webkit-scrollbar {
        width: 8px;
        background-color: #1e293b;
    }
    .log-list::-webkit-scrollbar-thumb {
        background-color: #334155;
        border-radius: 4px;
    }

    /* Ensure white text in code blocks and tables */
    pre {
        color: white !important;
    }
    .environment-table {
        color: white !important;
    }
    
    /* Hover states for log links */
    .log-list a {
        color: white !important;
        text-decoration: none;
    }
    .log-list a:hover {
        color: var(--bs-primary) !important;
    }

    /* Tab navigation styling */
    .nav-tabs .nav-link {
        color: white !important;
        border-color: var(--bs-border-color);
    }
    .nav-tabs .nav-link:hover {
        color: var(--bs-primary) !important;
        border-color: var(--bs-border-color);
    }
    .nav-tabs .nav-link.active {
        color: var(--bs-primary) !important;
        background-color: #1e293b;
        border-color: var(--bs-border-color);
    }

    /* Navigation text overflow prevention */
    #test-case-nav a,
    #error-list a {
        white-space: normal !important;
        word-wrap: break-word !important;
        word-break: break-word !important;
        padding: 8px 12px !important;
        line-height: 1.4 !important;
    }

    /* Ensure proper spacing in nav sidebar */
    .nav-sidebar .list-group-item {
        border-radius: 0;
        margin-bottom: 2px;
    }

    /* Common Errors list styling */
    #error-list {
        background-color: #1e293b;
        border-radius: 0.375rem;
        border: 1px solid var(--bs-border-color);
        margin-bottom: 1rem;
    }

    /* Test Cases navigation styling */
    #test-case-nav {
        max-height: calc(100vh - 400px);
        overflow-y: auto;
    }
</style>
"""


def generate_html_report():
    # Create directories and process logs
    (REPORT_DIR / "logs").mkdir(parents=True, exist_ok=True)
    process_log_files()

    # Generate test cases first to collect names
    test_cases_html = generate_test_cases()
    test_case_nav = generate_test_case_navigation()

    # Generate HTML content
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Test Report - {COMMIT_HASH[:8]}</title>
        {MODERN_CSS}
    </head>
    <body>
        <div class="d-flex">
            <!-- Navigation Sidebar -->
            <nav class="nav-sidebar">
                <h5 class="mt-4 mb-3 text-white">Common Errors</h5>
                <div id="error-list" class="list-group small">
                    {generate_error_summaries()}
                </div>
 
                <h5 class="mb-3 text-white">Test Cases</h5>
                <div id="test-case-nav" class="list-group small">
                    {test_case_nav}
                </div>
           </nav>

            <!-- Main Content -->
            <main class="main-content">
                <h1 class="mb-4">Test Report <small class="text-muted">{COMMIT_HASH[:8]}</small></h1>
                
                <!-- Search Bar -->
                <div class="search-container">
                    <input type="text" class="form-control" placeholder="Search test cases..." 
                           id="searchInput" onkeyup="filterTests()">
                </div>


                <div id="test-cases-section">
                    {test_cases_html}
                </div>
                
                <div class="card mt-4" id="logs-section">
                    <div class="card-body">
                        <h5 class="card-title">Log Files</h5>
                        <div class="log-list">
                            <ul class="list-group">
                                {generate_log_links()}
                            </ul>
                        </div>
                    </div>
                </div>
            </main>
        </div>

        <script>
            // Store last visited indices for each error pattern
            const errorVisitState = {{}};
            let allTestCards = [];
            
            // Initialize everything when DOM is loaded
            document.addEventListener('DOMContentLoaded', () => {{
                // Cache all test cards once
                allTestCards = Array.from(document.querySelectorAll('.test-card'));
                
                // Initialize error items
                document.querySelectorAll('.error-item').forEach(initializeErrorItem);
                
                // Initialize search
                const searchInput = document.getElementById('searchInput');
                if (searchInput) {{
                    searchInput.addEventListener('input', filterTests);
                }}
                
                // Initialize navigation links
                document.querySelectorAll('#test-case-nav a').forEach(link => {{
                    link.addEventListener('click', handleNavClick);
                }});
            }});
            
            function handleNavClick(e) {{
                e.preventDefault();
                const targetId = e.target.getAttribute('href');
                const targetElement = document.querySelector(targetId);
                if (targetElement) {{
                    targetElement.scrollIntoView({{
                        behavior: 'smooth',
                        block: 'start'
                    }});
                }}
            }}
            
            function filterTests() {{
                const filter = this.value.toLowerCase();
                allTestCards.forEach(card => {{
                    const text = card.textContent.toLowerCase();
                    card.style.display = text.includes(filter) ? 'block' : 'none';
                }});
            }}
            
            function initializeErrorItem(errorItem) {{
                const errorPattern = errorItem.getAttribute('data-error');
                const matchingCards = allTestCards.filter(
                    card => card.getAttribute('data-error') === errorPattern
                );
                
                // Update initial count in badge
                const badge = errorItem.querySelector('.badge');
                if (badge) {{
                    badge.textContent = `0/${{matchingCards.length}}`;
                }}
                
                errorVisitState[errorPattern] = {{
                    currentIndex: -1,
                    totalCards: matchingCards.length,
                    cards: matchingCards  // Cache matching cards
                }};
                
                errorItem.addEventListener('click', (e) => handleErrorClick(e, errorPattern));
            }}
            
            function handleErrorClick(e, errorPattern) {{
                e.preventDefault();
                const state = errorVisitState[errorPattern];
                const matchingCards = state.cards;
                
                if (matchingCards.length === 0) return;
                
                // Move to next card
                state.currentIndex = (state.currentIndex + 1) % matchingCards.length;
                const targetCard = matchingCards[state.currentIndex];
                
                // Update badge
                const badge = e.currentTarget.querySelector('.badge');
                if (badge) {{
                    badge.textContent = `${{state.currentIndex + 1}}/${{matchingCards.length}}`;
                }}
                
                // Remove existing highlights
                matchingCards.forEach(card => {{
                    card.style.boxShadow = '';
                }});
                
                // Scroll and highlight
                targetCard.scrollIntoView({{
                    behavior: 'smooth',
                    block: 'start'
                }});
                
                targetCard.style.boxShadow = '0 0 15px rgba(220, 53, 69, 0.5)';
                setTimeout(() => {{
                    targetCard.style.boxShadow = '';
                }}, 2000);
            }}
        </script>
        
        <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
    </body>
    </html>
    """

    (REPORT_DIR / "index.html").write_text(dedent(html_content))


def process_log_files():
    # Process both bazel logs and failed test logs
    for log_file in FAILED_LOGS_DIR.glob("*"):
        if log_file.suffix == ".zip":
            shutil.unpack_archive(log_file, REPORT_DIR / "logs" / log_file.name[:-4])


def generate_test_case_navigation():
    test_summaries_dir = Path(f"{BASE_DIR}/artifacts/test-summaries/")
    nav_items = []

    for idx, test_file in enumerate(test_summaries_dir.glob("*.txt")):
        if "000_header.txt" in test_file.name:
            continue

        with open(test_file, "r") as f:
            test_name = f.readline().strip().split(": ")[-1]
            pos = test_name.find("com_github_ray_project_ray")
            test_name = test_name[pos + len("com_github_ray_project_ray/") :]
            test_id = f"test-case-{idx}"
            nav_items.append(
                f'<a href="#{test_id}" class="list-group-item list-group-item-action py-2" style="white-space: normal;">'
                f"{test_name}</a>"
            )

    return "\n".join(nav_items)


def generate_test_cases():
    test_cases = []
    error_counts = {}
    test_summaries_dir = Path(f"{BASE_DIR}/artifacts/test-summaries/")

    for idx, test_file in enumerate(test_summaries_dir.glob("*.txt")):
        if "000_header.txt" in test_file.name:
            continue

        try:
            content = test_file.read_text()
            test_name = test_file.name.replace("bazel-out::", "").replace("::", "/")
            pos = test_name.find("com_github_ray_project_ray")
            test_name = test_name[pos + len("com_github_ray_project_ray/") :]

            # Improved error pattern extraction
            error_pattern = "No error"
            error_lines = [
                line.strip() for line in content.splitlines() if line.startswith("E   ")
            ]
            if error_lines:
                # Get last error line and extract key pattern
                error_line = error_lines[-1]
                error_pattern = re.sub(r"\b\w+Error\b", "", error_line).strip(" :")
                if not error_pattern:
                    error_pattern = error_line.split(":")[-1].strip()

                error_counts[error_pattern] = error_counts.get(error_pattern, 0) + 1

            test_id = f"test-case-{idx}"
            test_cases.append(
                f"""
            <div class="card test-card mb-4" id="{test_id}" data-error="{error_pattern}">
                <div class="card-body">
                    <h5 class="card-title text-danger">{test_name}</h5>
                    <div class="mb-3">
                        <span class="badge bg-danger">Failed</span>
                    </div>
                    
                    <ul class="nav nav-tabs mb-3">
                        <li class="nav-item">
                            <a class="nav-link active" data-bs-toggle="tab" href="#traceback-{test_file.stem}">Info</a>
                        </li>
                    </ul>
                    
                    <div class="tab-content">
                        <div class="tab-pane show active" id="traceback-{test_file.stem}">
                            <pre class="text-white">{content}</pre>
                        </div>
                    </div>
                </div>
            </div>
            """
            )
        except Exception as e:
            print(f"Error processing {test_file}: {str(e)}")
            continue

    generate_test_cases.error_counts = error_counts
    return "\n".join(test_cases)


def generate_error_summaries():
    error_counts = getattr(generate_test_cases, "error_counts", {})
    if not error_counts:
        return ""

    sorted_errors = sorted(error_counts.items(), key=lambda x: x[1], reverse=True)
    return "\n".join(
        f'<a href="#" class="list-group-item list-group-item-action error-item d-flex justify-content-between align-items-center py-2 text-truncate" data-error="{error}">'
        f'<span class="text-truncate">{error}</span><span class="badge bg-danger ms-2">{count}</span></a>'
        for error, count in sorted_errors
    )


def generate_log_links():
    links = []

    def process_directory(directory, relative_path="", depth=0):
        for path in sorted(directory.iterdir()):
            indent = "&nbsp;" * (depth * 4)
            if path.is_file():
                size = path.stat().st_size // 1024
                file_path = (
                    f"{relative_path}/{path.name}" if relative_path else path.name
                )
                links.append(
                    f"""
                <li class="list-group-item">
                    <div class="d-flex justify-content-between align-items-center">
                        <div>
                            {indent}📄 <a href="logs/{file_path}" class="text-white">
                                {path.name}
                            </a>
                        </div>
                        <span class="badge bg-primary rounded-pill">{size} KB</span>
                    </div>
                </li>
                """
                )
            elif path.is_dir():
                new_relative_path = (
                    f"{relative_path}/{path.name}" if relative_path else path.name
                )
                unique_id = f"dir_{new_relative_path.replace('/', '_')}"
                links.append(
                    f"""
                <li class="list-group-item">
                    <div class="d-flex align-items-center">
                        {indent}
                        <button class="btn btn-link p-0 me-1 text-white" 
                                type="button" 
                                data-bs-toggle="collapse" 
                                data-bs-target="#{unique_id}"
                                aria-expanded="false">
                            <span class="caret">▶</span>
                        </button>
                        📁 <span class="text-white">{path.name}/</span>
                    </div>
                    <div class="collapse" id="{unique_id}">
                        <ul class="list-group list-group-flush">
                """
                )
                process_directory(path, new_relative_path, depth + 1)
                links.append(
                    """
                        </ul>
                    </div>
                </li>
                """
                )

    process_directory(REPORT_DIR / "logs")
    return "\n".join(links)


if __name__ == "__main__":
    generate_html_report()
    print(f"Report generated at: {REPORT_DIR}/index.html")
