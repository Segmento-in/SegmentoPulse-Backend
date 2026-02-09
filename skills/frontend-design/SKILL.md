---
name: frontend-design
description: Guidelines for creating distinctive, production-grade frontend designs that avoid generic AI aesthetics.
---

# Frontend Design Skill

This skill guides the creation of high-quality, distinctive web interfaces. It emphasizes "anti-patterns" to avoid and encourages bold, professional design choices.

## Core Philosophy
1.  **Avoid "AI Slop"**: Do not output generic, cookie-cutter layouts. Avoid excessive whitespace without purpose, generic Bootstrap-like grids, and uninspired typography.
2.  **Distinctive Typography**: Use modern font stacks (Inter, Roboto, system-ui) with purposeful weight and spacing. High contrast and hierarchy are key.
3.  **Vibrant & Professional**: Use curated color palettes. Avoid default "red/blue/green". Use HSL/OKLCH for nuanced colors.

## Design Rules

### Layout & Composition
- **Asymmetry**: Use asymmetrical layouts to create visual interest.
- **Grids**: Use CSS Grid for complex, 2-dimensional layouts. Flexbox for 1D.
- **Whitespace**: Use whitespace intentionally to group related content, not just to fill space.

### Interaction & Feedback
- **Hover States**: Every interactive element MUST have a hover state.
- **Focus Rings**: Never remove focus rings without replacing them with a custom distinct style.
- **Transitions**: Use `transition: all 0.2s ease` (or specific properties) to smooth out state changes.

### Code Quality
- **Semantic HTML**: Use `<main>`, `<article>`, `<section>`, `<nav>`, `<aside>` correctly.
- **Mobile First**: Write CSS mobile-first (min-width media queries).
- **Clean CSS**: Avoid deep nesting. Use CSS variables for theme tokens (colors, spacing).

## Anti-Patterns (Do NOT do this)
- ❌ Using 100% generic border-radius (e.g., `border-radius: 4px` on everything). Vary it based on size.
- ❌ Using pure black (`#000`) or pure white (`#fff`) for backgrounds. Use off-black (`#111`/`#0f0f0f`) or off-white.
- ❌ Leaving button clicks without visual feedback (active state).
