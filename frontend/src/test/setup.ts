import '@testing-library/jest-dom/vitest'

// React Flow requires ResizeObserver in the test environment
if (typeof globalThis.ResizeObserver === 'undefined') {
  globalThis.ResizeObserver = class {
    observe() {}
    unobserve() {}
    disconnect() {}
  }
}
