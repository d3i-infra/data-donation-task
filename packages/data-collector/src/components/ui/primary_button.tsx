import React, { JSX } from 'react'

interface Props {
  label: string
  onClick: () => void
  color?: string
  enabled?: boolean
  spinning?: boolean
}

export const PrimaryButton = ({
  label,
  onClick,
  color = 'bg-primary text-white',
  enabled = true,
  spinning = false,
}: Props): JSX.Element => {
  const spinnerColor = color.includes('bg-tertiary') ? 'text-grey1' : 'text-white'

  return (
    <div role='button' className='relative'>
      <div
        className={`flex flex-col items-center leading-none font-button text-button rounded ${enabled ? 'cursor-pointer active:shadow-top4px' : ''} ${color}`}
        onClick={onClick}
      >
        <div
          className={`pt-15px pb-15px pr-4 pl-4 ${enabled ? 'active:pt-4 active:pb-14px' : ''} ${spinning ? 'opacity-0' : ''}`}
        >
          {label}
        </div>
      </div>
      <div className={`absolute top-0 h-full w-full flex flex-col justify-center items-center ${spinning ? '' : 'hidden'}`}>
        <svg
          className={`animate-spin w-4 h-4 ${spinnerColor}`}
          xmlns='http://www.w3.org/2000/svg'
          fill='none'
          viewBox='0 0 24 24'
        >
          <circle className='opacity-25' cx='12' cy='12' r='10' stroke='currentColor' strokeWidth='4' />
          <path
            className='opacity-75'
            fill='currentColor'
            d='M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z'
          />
        </svg>
      </div>
    </div>
  )
}
