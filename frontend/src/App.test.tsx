import { render, screen } from '@testing-library/react'
import { expect, test } from 'vitest'

import { App } from './App'

test('renders the local setup placeholder', () => {
  render(<App />)

  expect(screen.getByRole('heading', { name: 'Time-Series Virtual Lab' })).toBeInTheDocument()
  expect(screen.getByText('Local development setup is ready.')).toBeInTheDocument()
})
