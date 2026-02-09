---
name: web-design-guidelines
description: foundational guidelines for robust, accessible, and performant web design.
---

# Web Design Guidelines Skill

This skill provides the non-negotiable foundations for professional web development. While "Pro Max" focuses on aesthetics, this skill focuses on *quality* and *usability*.

## 1. Responsive Design
- **Fluid Layouts**: Use percentages (`%`), viewport units (`vw`, `vh`), or flex/grid `fr` units. Avoid fixed pixel widths for containers.
- **Media Queries**:
  - Use standard breakpoints (e.g., Mobile: <640px, Tablet: <1024px, Desktop: >1024px).
  - **Mobile First**: Define base styles for mobile, then override for larger screens (`@media (min-width: 640px) { ... }`).

## 2. Accessibility (A11y)
- **Headings**: Use `h1` through `h6` sequentially. Do not skip levels.
- **Contrast**: Text must have a contrast ratio of at least 4.5:1 against the background (WCAG AA).
- **Keyboard Navigation**: Ensure all interactive elements (buttons, links, inputs) are reachable via Tab key and have visible `:focus` states.
- **Alt Text**: All meaningful images MUST have `alt` attributes. Decorative images should have `alt=""`.

## 3. Performance
- **Images**: Use WebP or AVIF formats. Use lazy loading (`loading="lazy"`) for images below the fold.
- **Minimization**: CSS and JS should be minimized in production.
- **Fonts**: Preconnect to font sources. Limit the number of font weights loaded.

## 4. SEO Basics
- **Title & Meta**: Every page must have a unique `<title>` and `<meta name="description">`.
- **Semantic Structure**: Search engines rely on semantic tags (`nav`, `main`, `footer`) to understand page structure.
- **Clean URLs**: Use descriptive, kebab-case URLs (`/about-us` not `/p?id=123`).
