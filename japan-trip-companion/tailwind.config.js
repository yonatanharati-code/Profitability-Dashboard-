/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  theme: {
    extend: {
      colors: {
        paper: '#FAF8F4',
        surface: '#FFFFFF',
        sumi: {
          50: '#F4F3F1',
          100: '#E6E3DE',
          200: '#CFCAC2',
          300: '#A9A29A',
          400: '#7C756D',
          500: '#5A544E',
          600: '#413C38',
          700: '#2E2A27',
          800: '#211E1C',
          900: '#161413',
        },
        shu: {
          50: '#FCF1EF',
          100: '#F7DED9',
          400: '#C4574A',
          500: '#B0453A',
          600: '#93362D',
        },
        sage: {
          50: '#F1F4F0',
          100: '#DFE6DC',
          400: '#7E9678',
          500: '#657D5F',
          600: '#4F634A',
        },
        ai: {
          50: '#EEF2F5',
          100: '#DAE3EA',
          400: '#6E8CA0',
          500: '#587488',
          600: '#455C6C',
        },
        gold: {
          50: '#FBF5E9',
          100: '#F3E6C9',
          500: '#A8843C',
          600: '#8A6B2E',
        },
      },
      fontFamily: {
        sans: ['"Inter var"', 'Inter', 'system-ui', '-apple-system', 'Segoe UI', 'sans-serif'],
        display: ['"Zen Kaku Gothic New"', 'Inter', 'system-ui', 'sans-serif'],
        serif: ['"Zen Old Mincho"', 'Georgia', 'serif'],
      },
      borderRadius: {
        card: '18px',
        pill: '999px',
      },
      boxShadow: {
        card: '0 1px 2px rgba(33,30,28,0.04), 0 6px 20px -8px rgba(33,30,28,0.10)',
        lift: '0 2px 4px rgba(33,30,28,0.05), 0 16px 36px -12px rgba(33,30,28,0.18)',
        nav: '0 -1px 0 rgba(33,30,28,0.06), 0 -10px 30px -18px rgba(33,30,28,0.25)',
      },
      keyframes: {
        'fade-up': {
          '0%': { opacity: '0', transform: 'translateY(6px)' },
          '100%': { opacity: '1', transform: 'translateY(0)' },
        },
        'expand': {
          '0%': { opacity: '0', maxHeight: '0' },
          '100%': { opacity: '1', maxHeight: '2000px' },
        },
      },
      animation: {
        'fade-up': 'fade-up 260ms cubic-bezier(0.22,1,0.36,1) both',
        'expand': 'expand 240ms ease-out both',
      },
    },
  },
  plugins: [],
}
