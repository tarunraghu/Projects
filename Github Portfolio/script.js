// GitHub API configuration
const GITHUB_API_BASE = 'https://api.github.com';
const GITHUB_USERNAME = 'tarunraghu'; // Your GitHub username

// DOM Elements
const projectsContainer = document.getElementById('projects-container');

// Fetch GitHub projects
async function fetchGitHubProjects() {
    try {
        // Add headers to handle CORS
        const response = await fetch(`${GITHUB_API_BASE}/users/${GITHUB_USERNAME}/repos?sort=updated&direction=desc`, {
            method: 'GET',
            headers: {
                'Accept': 'application/vnd.github.v3+json',
                'Content-Type': 'application/json'
            }
        });
        
        if (!response.ok) {
            throw new Error(`GitHub API error: ${response.status} ${response.statusText}`);
        }
        
        const projects = await response.json();
        
        // Filter out forked repositories and sort by stars
        const filteredProjects = projects
            .filter(project => !project.fork)
            .sort((a, b) => b.stargazers_count - a.stargazers_count)
            .slice(0, 6); // Display top 6 projects

        if (filteredProjects.length === 0) {
            projectsContainer.innerHTML = '<p>No projects found. Please check your GitHub username.</p>';
            return;
        }

        displayProjects(filteredProjects);
    } catch (error) {
        console.error('Error fetching GitHub projects:', error);
        projectsContainer.innerHTML = `
            <div class="error-message">
                <p>Error loading projects: ${error.message}</p>
                <p>Please check your internet connection and try again later.</p>
            </div>
        `;
    }
}

// Display projects in the grid
function displayProjects(projects) {
    projectsContainer.innerHTML = projects.map(project => `
        <div class="project-card">
            <div class="project-content">
                <h3>${project.name}</h3>
                <p>${project.description || 'No description available'}</p>
                <div class="project-stats">
                    <span><i class="fas fa-star"></i> ${project.stargazers_count}</span>
                    <span><i class="fas fa-code-branch"></i> ${project.forks_count}</span>
                </div>
                <div class="project-links">
                    <a href="${project.html_url}" target="_blank" class="btn primary">View Project</a>
                    ${project.homepage ? `<a href="${project.homepage}" target="_blank" class="btn secondary">Live Demo</a>` : ''}
                </div>
            </div>
        </div>
    `).join('');
}

// Smooth scrolling for navigation links
document.querySelectorAll('a[href^="#"]').forEach(anchor => {
    anchor.addEventListener('click', function (e) {
        e.preventDefault();
        const target = document.querySelector(this.getAttribute('href'));
        if (target) {
            target.scrollIntoView({
                behavior: 'smooth',
                block: 'start'
            });
        }
    });
});

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    fetchGitHubProjects();
}); 