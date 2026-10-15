import { render, screen } from '@testing-library/react'
import { expect, test } from 'vitest'

import { App } from './App'

test('renders the simulation shell and controls', () => {
  render(<App />)

  expect(screen.getAllByText('FLOWSTORE').length).toBeGreaterThan(0)
  expect(screen.getByRole('button', { name: 'Start' })).toBeInTheDocument()
  expect(screen.getByText('Generator')).toBeInTheDocument()
  expect(screen.getByText('Queue')).toBeInTheDocument()
  expect(screen.getByText('Processor')).toBeInTheDocument()
  expect(screen.getByText('Buffers')).toBeInTheDocument()
  expect(screen.getByText('Chunk Builder')).toBeInTheDocument()
  expect(screen.getByText('Parquet Writer')).toBeInTheDocument()
  expect(screen.getByText('Local Storage')).toBeInTheDocument()
})
