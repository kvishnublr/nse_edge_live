/** @type {import('tailwindcss').Config} */
/* Content scan: single SPA; extend colors map to CSS variables for utilities */
module.exports = {
  content: ['./index.html'],
  corePlugins: {
    preflight: false,
  },
  theme: {
    extend: {
      colors: {
        canvas: 'var(--bg)',
        surface: {
          DEFAULT: 'var(--s2)',
          1: 'var(--s1)',
          2: 'var(--s2)',
          3: 'var(--s3)',
          4: 'var(--s4)',
        },
        foreground: {
          DEFAULT: 'var(--t0)',
          secondary: 'var(--t1)',
          muted: 'var(--t2)',
          subtle: 'var(--t3)',
        },
        edge: {
          DEFAULT: 'var(--b1)',
          strong: 'var(--b2)',
        },
        success: {
          DEFAULT: 'var(--go)',
          surface: 'var(--go-d)',
          border: 'var(--go-b)',
        },
        danger: {
          DEFAULT: 'var(--st)',
          surface: 'var(--st-d)',
          border: 'var(--st-b)',
        },
        warning: {
          DEFAULT: 'var(--am)',
          surface: 'var(--am-d)',
          border: 'var(--am-b)',
        },
        info: {
          DEFAULT: 'var(--bl)',
          surface: 'var(--bl-d)',
          border: 'var(--bl-b)',
        },
      },
      fontFamily: {
        sans: ['var(--fn)', 'Inter', 'ui-sans-serif', 'system-ui', 'sans-serif'],
        display: ['var(--fh)', 'Inter', 'ui-sans-serif', 'system-ui', 'sans-serif'],
      },
      fontSize: {
        xs: ['12px', { lineHeight: '1.5' }],
        sm: ['13px', { lineHeight: '1.5' }],
        base: ['14px', { lineHeight: '1.5' }],
        lg: ['16px', { lineHeight: '1.45' }],
        xl: ['20px', { lineHeight: '1.35' }],
        '2xl': ['24px', { lineHeight: '1.25' }],
        '3xl': ['32px', { lineHeight: '1.15' }],
      },
      borderRadius: {
        ds: '10px',
        'ds-sm': '8px',
        'ds-lg': '12px',
      },
      boxShadow: {
        ds: 'var(--shadow-md)',
        'ds-sm': 'var(--shadow-sm)',
        focus: 'var(--focus-ring)',
      },
      transitionDuration: {
        ds: '150ms',
      },
    },
  },
  plugins: [],
};
