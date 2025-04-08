# GitHub Portfolio

A modern, responsive portfolio website that showcases your GitHub projects and skills. This portfolio is built with HTML, CSS, and JavaScript, and automatically fetches your GitHub repositories to display them in a beautiful grid layout.

## Features

- 🎨 Modern and clean design
- 📱 Fully responsive layout
- ⚡ Fast and lightweight
- 🔄 Dynamic GitHub project loading
- 🎯 Smooth scrolling navigation
- 💼 Skills section
- 📧 Contact section with social links

## Getting Started

1. Clone this repository:
```bash
git clone https://github.com/yourusername/github-portfolio.git
cd github-portfolio
```

2. Customize the content:
   - Open `index.html` and update the personal information
   - Modify the skills in the About section
   - Update social media links in the Contact section
   - Replace `yourusername` in `script.js` with your GitHub username

3. Deploy your portfolio:
   - You can deploy this portfolio on GitHub Pages, Netlify, or any other hosting service
   - For GitHub Pages, simply push your code to a repository and enable GitHub Pages in the repository settings

## Customization

### Colors
The color scheme can be modified in the `:root` section of `styles.css`:
```css
:root {
    --primary-color: #2d3436;
    --secondary-color: #0984e3;
    --text-color: #2d3436;
    --background-color: #ffffff;
    --accent-color: #00b894;
}
```

### Projects
The number of projects displayed can be adjusted in `script.js` by modifying the `slice(0, 6)` value in the `fetchGitHubProjects` function.

### Skills
Add or remove skills by modifying the skill tags in the About section of `index.html`.

## Contributing

Feel free to submit issues and enhancement requests!

## License

This project is licensed under the MIT License - see the LICENSE file for details. 