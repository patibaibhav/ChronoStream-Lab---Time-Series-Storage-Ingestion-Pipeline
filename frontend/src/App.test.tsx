import { render, screen } from '@testing-library/react'
import { expect, test } from 'vitest'

import { App } from './App'

test('renders the simulation shell and controls', () => {
  render(<App />)

  expect(screen.getAllByText('FLOWSTORE').length).toBeGreaterThan(0)
  expect(screen.getByRole('button', { name: 'Start' })).toBeInTheDocument()
  expect(screen.getByText(/Baseline Ingestion/i)).toBeInTheDocument()
  expect(screen.getByText(/High Throughput/i)).toBeInTheDocument()
  expect(screen.getByText(/Slow Storage/i)).toBeInTheDocument()
  expect(screen.getByText(/Queue Saturation/i)).toBeInTheDocument()
  expect(screen.getByText(/Invalid Records/i)).toBeInTheDocument()
  expect(screen.getByText(/Run History/i)).toBeInTheDocument()
  expect(screen.getByText(/Event Log/i)).toBeInTheDocument()
  expect(screen.getAllByText(/Storage Explorer/i).length).toBeGreaterThan(0)
})
