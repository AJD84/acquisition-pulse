# Acquisition Pulse Website

This directory contains the marketing website for Acquisition Pulse, built with clean HTML/CSS for GitHub Pages deployment.

## 📁 Directory Structure

```
website/
├── index.html           # Landing page with hero, value prop, comparison
├── getting-started.html # Step-by-step setup guide for Shopify + Google Ads
├── privacy.html         # Privacy policy (required for Google API approval)
├── styles.css           # Global stylesheet
├── assets/              # Logo files and images
│   ├── logo-light.png   # Primary logo (176KB, transparent)
│   ├── logo-dark.png    # Dark sections logo (163KB, transparent)
│   └── favicon.png      # Browser favicon (66KB, 512x512px)
└── README.md            # This file
```

## 🚀 Deployment to GitHub Pages

### Option 1: Deploy from Main Branch (Recommended)

1. **Create a new GitHub repository:**
   ```bash
   # From the project root directory
   cd /Users/javiermina/Downloads/Profit_Dashboard_Project
   git init
   git add .
   git commit -m "Initial commit with dashboard and website"
   ```

2. **Push to GitHub:**
   ```bash
   # Create a new repo on GitHub first, then:
   git remote add origin https://github.com/YOUR_USERNAME/acquisition-pulse.git
   git branch -M main
   git push -u origin main
   ```

3. **Enable GitHub Pages:**
   - Go to your repository on GitHub
   - Click **Settings** → **Pages**
   - Under "Source", select **Deploy from a branch**
   - Select branch: `main`
   - Select folder: `/website`
   - Click **Save**

4. **Access your site:**
   - Your site will be live at: `https://YOUR_USERNAME.github.io/acquisition-pulse/`
   - It may take a few minutes to deploy

### Option 2: Deploy from Docs Folder

If you prefer to use the `/docs` folder convention:

1. **Rename the website folder:**
   ```bash
   mv website docs
   ```

2. **Follow steps 1-2 from Option 1**

3. **Enable GitHub Pages:**
   - In Settings → Pages
   - Select folder: `/docs` instead of `/website`

### Option 3: Deploy to Custom Domain

1. **Follow Option 1 or 2 to deploy to GitHub Pages first**

2. **Add a custom domain:**
   - In your repository, go to Settings → Pages
   - Under "Custom domain", enter your domain (e.g., `acquisitionpulse.com`)
   - Click **Save**

3. **Configure DNS:**
   - Add a CNAME record pointing to `YOUR_USERNAME.github.io`
   - Or for apex domain, add A records to GitHub Pages IPs:
     ```
     185.199.108.153
     185.199.109.153
     185.199.110.153
     185.199.111.153
     ```

4. **Wait for DNS propagation** (can take up to 24 hours)

## 🎨 Customization

### Update Logo
Replace files in `assets/` folder:
- `logo-light.png` - Used in header and light backgrounds
- `logo-dark.png` - Used in dark sections (optional)
- `favicon.png` - Browser tab icon

### Edit Colors
In `styles.css`, modify CSS variables in the `:root` section:
```css
:root {
    --navy: #1E3A8A;       /* Primary brand color */
    --orange: #FB8C00;     /* Accent/CTA color */
    --navy-light: #3B82F6; /* Hover states */
    --orange-light: #FFA726;
}
```

### Add Analytics
To track visitors, add Google Analytics or Plausible before `</head>`:
```html
<!-- Google Analytics -->
<script async src="https://www.googletagmanager.com/gtag/js?id=G-XXXXXXXXXX"></script>
<script>
  window.dataLayer = window.dataLayer || [];
  function gtag(){dataLayer.push(arguments);}
  gtag('js', new Date());
  gtag('config', 'G-XXXXXXXXXX');
</script>
```

## 📝 Content Updates

### Landing Page (`index.html`)
- **Hero headline:** Line 26 - "Know Your True Profitability"
- **Problem cards:** Lines 38-56 - Why revenue metrics mislead
- **Comparison table:** Lines 73-114 - Acquisition Pulse vs typical dashboards
- **How it works:** Lines 122-144 - 3-step process
- **Benefits:** Lines 152-193 - Value propositions

### Getting Started (`getting-started.html`)
- **Shopify export guide:** Lines 50-78
- **CSV requirements:** Lines 85-123 - Required columns (order_date, revenue, cogs, platform)
- **Upload instructions:** Lines 151-167
- **Google Ads connection:** Lines 173-196

### Privacy Policy (`privacy.html`)
- **Contact email:** Line 172 - Update `privacy@acquisitionpulse.com` to your email
- **GitHub repository:** Line 173 - Add your GitHub repo URL
- **Compliance sections:** Lines 187-234 - GDPR/CCPA info

## 🔗 URLs for Google API Approval

When applying for Google Ads API Basic/Standard access, use these URLs:

1. **Application Homepage:**
   ```
   https://YOUR_USERNAME.github.io/acquisition-pulse/
   ```

2. **Privacy Policy URL:**
   ```
   https://YOUR_USERNAME.github.io/acquisition-pulse/privacy.html
   ```

3. **Terms of Service URL (if required):**
   Create `terms.html` similar to `privacy.html`

## 🧪 Local Testing

To test the website locally before deploying:

### Option 1: Python Simple Server
```bash
cd /Users/javiermina/Downloads/Profit_Dashboard_Project/website
python3 -m http.server 8080
# Visit http://localhost:8080
```

### Option 2: VS Code Live Server
1. Install "Live Server" extension in VS Code
2. Right-click `index.html`
3. Select "Open with Live Server"

## ✅ Pre-Deployment Checklist

- [ ] Test all pages locally
- [ ] Verify logo files display correctly
- [ ] Check all internal links work
- [ ] Update contact email in `privacy.html`
- [ ] Add GitHub repository URL
- [ ] Test mobile responsiveness (use browser dev tools)
- [ ] Optimize images if needed (logos are already optimized)
- [ ] Spell-check all content
- [ ] Add Google Analytics (optional)

## 🐛 Troubleshooting

**Logo images not loading:**
- Ensure `assets/` folder is in the same directory as HTML files
- Check file paths are relative: `assets/logo-light.png`
- Verify PNG files are not corrupted

**GitHub Pages not deploying:**
- Check repository is public (or you have GitHub Pro for private repos)
- Verify GitHub Pages is enabled in Settings → Pages
- Wait 5-10 minutes for initial deployment
- Check Actions tab for build errors

**CSS not applying:**
- Clear browser cache (Cmd+Shift+R on Mac, Ctrl+Shift+R on Windows)
- Verify `styles.css` is in the same directory as HTML files
- Check browser console for 404 errors

## 📧 Support

For questions about the website:
- Open an issue on GitHub
- Email: [your-email@example.com]

## 📄 License

This website template is part of the Acquisition Pulse project.
[Add your license here, e.g., MIT License]
# Updated Fri Dec 12 14:54:09 AEST 2025
