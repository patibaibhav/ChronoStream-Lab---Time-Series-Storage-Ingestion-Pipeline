import { render, screen } from '@testing-library/react'
import { expect, test } from 'vitest'

import { App } from './App'

test('renders the simulation shell and controls', () => {
  render(<App />)

  expect(screen.getByRole('heading', { name: 'Time-Series Virtual Lab' })).toBeInTheDocument()
  expect(screen.getByRole('button', { name: 'Start simulation' })).toBeInTheDocument()
  expect(screen.getByText('Generator')).toBeInTheDocument()
})
