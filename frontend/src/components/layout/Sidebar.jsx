import { useState } from 'react'
import { Link, useLocation } from 'react-router-dom'
import { cn } from '@/lib/utils.js'
import {
  LayoutDashboard,
  CheckCircle,
  Settings,
  ListChecks,
  History,
  Menu,
  X
} from 'lucide-react'
import { Button } from '@/components/ui/button.jsx'
import { ThemeToggle } from '@/components/ThemeToggle.jsx'

const menuItems = [
  { text: 'Dashboard', icon: LayoutDashboard, path: '/' },
  { text: 'Stream Checker', icon: CheckCircle, path: '/stream-checker' },
  { text: 'Channel Configuration', icon: ListChecks, path: '/channels' },
  { text: 'Automation Settings', icon: Settings, path: '/settings' },
  { text: 'Changelog', icon: History, path: '/changelog' },
]

export function Sidebar() {
  const [isOpen, setIsOpen] = useState(false)
  const location = useLocation()

  return (
    <>
      {/* Mobile menu button */}
      <Button
        variant="ghost"
        size="icon"
        className="fixed top-4 left-4 z-50 lg:hidden"
        onClick={() => setIsOpen(!isOpen)}
      >
        {isOpen ? <X className="h-6 w-6" /> : <Menu className="h-6 w-6" />}
      </Button>

      {/* Overlay for mobile */}
      {isOpen && (
        <div
          className="fixed inset-0 bg-black/50 z-40 lg:hidden"
          onClick={() => setIsOpen(false)}
        />
      )}

      {/* Sidebar */}
      <aside
        className={cn(
          "fixed top-0 left-0 h-full w-64 bg-card border-r border-border z-40 transition-transform duration-300 ease-in-out flex flex-col",
          isOpen ? "translate-x-0" : "-translate-x-full lg:translate-x-0"
        )}
      >
        <div className="p-6">
          <h1 className="text-2xl font-bold text-primary">StreamFlow</h1>
          <p className="text-sm text-muted-foreground">for Dispatcharr</p>
        </div>

        <nav className="px-3 space-y-1 flex-1">
          {menuItems.map((item) => {
            const Icon = item.icon
            const isActive = location.pathname === item.path
            return (
              <Link
                key={item.path}
                to={item.path}
                onClick={() => setIsOpen(false)}
                className={cn(
                  "flex items-center gap-3 px-3 py-2 rounded-lg transition-colors",
                  isActive
                    ? "bg-primary text-primary-foreground"
                    : "hover:bg-accent hover:text-accent-foreground"
                )}
              >
                <Icon className="h-5 w-5" />
                <span className="text-sm font-medium">{item.text}</span>
              </Link>
            )
          })}
        </nav>

        <div className="p-3 border-t border-border">
          <div className="flex items-center justify-between">
            <span className="text-sm text-muted-foreground">Theme</span>
            <ThemeToggle />
          </div>
        </div>
      </aside>
    </>
  )
}
