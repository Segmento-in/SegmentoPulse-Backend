---
name: ui-ux-pro-max
description: Advanced UI/UX guidelines for creating premium, "wow-factor" web experiences with deep reasoning.
---

# UI/UX Pro Max Skill

This skill enforces a "Premium Only" mindset. It pushes for state-of-the-art visuals, glassmorphism, advanced animations, and deep user-centric reasoning.

## Reasoning Engine
Before implementing any UI, ask:
1.  **Is this premium?** Does it feel like a top-tier SaaS or consumer app?
2.  **Is it alive?** Does the interface react to the user? (Micro-interactions)
3.  **Is it accessible?** fast and readable?

## Visual Excellence

### Effects & Depth
- **Glassmorphism**: Use backdrop-filter: blur() with subtle translucent backgrounds for overlays and sticky headers.
- **Shadows**: Layer multiple shadows for realistic depth. Avoid single, harsh shadows.
  ```css
  /* Example Premium Shadow */
  box-shadow:
    0 1px 2px rgba(0,0,0,0.05),
    0 4px 8px rgba(0,0,0,0.05),
    0 12px 24px rgba(0,0,0,0.05);
  ```
- **Gradients**: Use subtle mesh gradients or noise textures to add richness.

### Motion Design
- **Entrance Animations**: Elements should fade/slide in smoothly.
- **Micro-interactions**: Buttons should scale down slightly on click (`transform: scale(0.98)`).
- **Staggering**: Stagger animations for lists or grids.

## UX Best Practices
- **Optimistic UI**: Update the UI immediately on action, then validate with the server.
- **Empty States**: Never leave a list empty. Show a helpful illustration and a CTA.
- **Loading Skeletons**: Use skeleton screens instead of generic spinners for initial load.

## Implementation Guide
- **Variables**: Define a `variables.css` with HSL color values for easy theming (Dark/Light mode).
- **Touch Targets**: Ensure all interactive elements are at least 44x44px on mobile.
- **Contrast**: Maintain WCAG AA standard for text contrast.
