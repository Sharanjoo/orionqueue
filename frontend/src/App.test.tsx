import { render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import App from './App'

describe('App', () => {
  it('renders the OrionQueue heading', () => {
    render(<App />)
    expect(screen.getByRole('heading', { name: 'OrionQueue', level: 1 })).toBeInTheDocument()
  })

  it('lists Phase 1 as in progress', () => {
    render(<App />)
    const phase1 = screen.getByText('Project foundation').closest('li')
    expect(phase1).not.toBeNull()
    expect(phase1).toHaveTextContent('in progress')
  })

  it('points readers at PROJECT_STATUS.md rather than showing fake data', () => {
    render(<App />)
    expect(screen.getByText('PROJECT_STATUS.md')).toBeInTheDocument()
  })
})
