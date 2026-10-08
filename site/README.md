# blockstor.org

The project website and documentation, built with [Hugo](https://gohugo.io/) and the [Docsy](https://www.docsy.dev/) theme. Production is published to GitHub Pages from `main`; pull requests get a Netlify deploy preview.

## Run it locally

You need Hugo extended (0.161 or newer), Go and Node.js 22. From this directory run `npm install` once, then `hugo server` and open http://localhost:1313/.

## Where things go

Documentation pages live under `content/en/docs/`, one markdown file per page with `title`, `weight` and `description` in the front matter; the weight sets the order in the sidebar. The landing page is `content/en/_index.md`. Write one continuous line per paragraph.
