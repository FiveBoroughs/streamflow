import { useState, useEffect, useCallback, useRef } from 'react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card.jsx'
import { Button } from '@/components/ui/button.jsx'
import { Badge } from '@/components/ui/badge.jsx'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table.jsx'
import { Input } from '@/components/ui/input.jsx'
import { Label } from '@/components/ui/label.jsx'
import { Switch } from '@/components/ui/switch.jsx'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select.jsx'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs.jsx'
import { Alert, AlertDescription } from '@/components/ui/alert.jsx'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover.jsx'
import { Command, CommandEmpty, CommandGroup, CommandInput, CommandItem, CommandList } from '@/components/ui/command.jsx'
import { useToast } from '@/hooks/use-toast.js'
import { eventOrderingAPI, channelsAPI } from '@/services/api.js'
import {
  ArrowUpDown, Plus, Trash2, Loader2, Play, Square, RefreshCw,
  CheckCircle2, XCircle, Clock, ChevronsUpDown, Check, Copy,
  Save, TestTube, Settings, Zap, ArrowUp, ArrowDown, Minus, X
} from 'lucide-react'
import { cn } from '@/lib/utils.js'

// Color-coded categories for the drag-drop regex builder
const CATEGORIES = [
  { id: 'league', label: 'League', color: '#f97316', tooltip: 'Event type (UFC, NBA, etc.)' },
  { id: 'team1', label: 'Team 1', color: '#0ea5e9', tooltip: 'Home team / first team' },
  { id: 'team2', label: 'Team 2', color: '#14b8a6', tooltip: 'Away team / second team' },
  { id: 'order', label: 'Order', color: '#a855f7', tooltip: 'Event number for tiebreaking' },
  { id: 'year', label: 'Year', color: '#3b82f6' },
  { id: 'month', label: 'Month', color: '#8b5cf6' },
  { id: 'day', label: 'Day', color: '#06b6d4' },
  { id: 'hour', label: 'Hour', color: '#10b981' },
  { id: 'minute', label: 'Minute', color: '#f59e0b' },
  { id: 'second', label: 'Second', color: '#ef4444' },
  { id: 'ampm', label: 'AM/PM', color: '#ec4899' },
  { id: 'ignore', label: 'Ignore', color: '#6b7280' },
]

// Muted color palette for channel badges
const CHANNEL_COLORS = [
  { bg: '#2d3748', border: '#4a5568', text: '#e2e8f0' },
  { bg: '#2c3e50', border: '#34495e', text: '#ecf0f1' },
  { bg: '#3d3d3d', border: '#5a5a5a', text: '#e0e0e0' },
  { bg: '#1e3a5f', border: '#2980b9', text: '#aed6f1' },
  { bg: '#4a2c2a', border: '#6b3a38', text: '#f5b7b1' },
  { bg: '#2e4a3e', border: '#27ae60', text: '#a9dfbf' },
  { bg: '#4a3f2e', border: '#d4a574', text: '#fdebd0' },
  { bg: '#3e2a4a', border: '#8e44ad', text: '#d7bde2' },
  { bg: '#2a3e4a', border: '#5dade2', text: '#aed6f1' },
  { bg: '#4a3e2a', border: '#f39c12', text: '#fdebd0' },
]

const escapeRegex = (str) => str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')

const buildCapturePattern = (text, category) => {
  switch (category) {
    case 'league': return '[A-Za-z]+'
    case 'team1': case 'team2': return text.includes(' ') ? '[A-Za-z0-9][A-Za-z0-9 ]*[A-Za-z0-9]' : '[A-Za-z0-9]+'
    case 'order': return '\\d+'
    case 'year': return '\\d{4}'
    case 'month': return /^\d+$/.test(text) ? '\\d{1,2}' : '[A-Za-z]+'
    case 'day': return '\\d{1,2}'
    case 'hour': return '\\d{1,2}'
    case 'minute': return '\\d{2}'
    case 'second': return '\\d{2}'
    case 'ampm': return '[AaPp][Mm]'
    default: return escapeRegex(text)
  }
}

const MONTHS = { jan:0, feb:1, mar:2, apr:3, may:4, jun:5, jul:6, aug:7, sep:8, oct:9, nov:10, dec:11 }
const TOKEN_SEPARATOR_CHARS = ['-', ':', '/', '@', '|', '(', ')', '#']
const TOKEN_SEPARATOR_CLASS = TOKEN_SEPARATOR_CHARS
  .map(char => ['\\', '-', ']'].includes(char) ? `\\${char}` : char)
  .join('')
const TOKEN_SPLIT_REGEX = new RegExp(
  `(\\s+|[${TOKEN_SEPARATOR_CLASS}]|(?<=\\d)(?=[AaPp][Mm]))`
)

function parseGroupsToDate(groups, timezone = null) {
  if (!groups) return null
  try {
    const now = new Date()
    const year = groups.year ? parseInt(groups.year) : now.getFullYear()

    // Support both 'month' and 'month2' named groups (like backend does)
    let month = now.getMonth()
    const monthStr = groups.month || groups.month2
    if (monthStr) {
      if (/^\d+$/.test(monthStr)) {
        month = parseInt(monthStr) - 1
      } else {
        month = MONTHS[monthStr.toLowerCase().slice(0, 3)] ?? now.getMonth()
      }
    }

    // Support 'day', 'date', and 'day2' named groups
    // Note: Check 'date' before 'day2' because 'day2' might be a day name like "Sat"
    const dayStr = groups.day || groups.date || groups.date2 || groups.day2
    const day = dayStr ? parseInt(dayStr) : now.getDate()

    let hour = groups.hour ? parseInt(groups.hour) : (groups.hour2 ? parseInt(groups.hour2) : 0)
    if (groups.ampm) {
      const ampm = groups.ampm.toUpperCase()
      if (ampm === 'PM' && hour !== 12) hour += 12
      if (ampm === 'AM' && hour === 12) hour = 0
    }
    const minute = groups.minute ? parseInt(groups.minute) : (groups.minute2 ? parseInt(groups.minute2) : 0)
    const second = groups.second ? parseInt(groups.second) : 0

    // Validate that we have valid numbers
    if (isNaN(day) || isNaN(month) || isNaN(year) || isNaN(hour) || isNaN(minute)) {
      return null
    }

    if (timezone) {
      try {
        // Interpret the parsed time as being in the given IANA timezone.
        // Trick: treat the numbers as UTC, format that UTC instant in the target tz,
        // compute the offset, then subtract it to get the real UTC timestamp.
        const utcMs = Date.UTC(year, month, day, hour, minute, second)
        const parts = new Intl.DateTimeFormat('en-US', {
          timeZone: timezone,
          year: 'numeric', month: '2-digit', day: '2-digit',
          hour: '2-digit', minute: '2-digit', second: '2-digit',
          hour12: false,
        }).formatToParts(new Date(utcMs))
        const p = {}
        parts.forEach(({ type, value }) => { p[type] = value })
        const tzMs = Date.UTC(
          parseInt(p.year), parseInt(p.month) - 1, parseInt(p.day),
          parseInt(p.hour) % 24, parseInt(p.minute), parseInt(p.second)
        )
        const date = new Date(utcMs - (tzMs - utcMs))
        return isNaN(date.getTime()) ? null : date
      } catch {
        // Fall through to local-time fallback
      }
    }

    const date = new Date(year, month, day, hour, minute, second)
    return isNaN(date.getTime()) ? null : date
  } catch {
    return null
  }
}

function formatCountdown(eventTime) {
  if (!eventTime) return '—'
  const now = new Date()
  const diff = eventTime - now
  const absMins = Math.floor(Math.abs(diff) / 60000)
  const hours = Math.floor(absMins / 60)
  const days = Math.floor(hours / 24)
  const rh = hours % 24
  const rm = absMins % 60

  if (diff < 0) {
    if (days > 0) return rh > 0 ? `${days}d ${rh}h ago` : `${days}d ago`
    if (hours > 0) return rm > 0 ? `${hours}h ${rm}m ago` : `${hours}h ago`
    return `${absMins}m ago`
  } else {
    if (days > 0) return rh > 0 ? `in ${days}d ${rh}h` : `in ${days}d`
    if (hours > 0) return rm > 0 ? `in ${hours}h ${rm}m` : `in ${hours}h`
    return `in ${absMins}m`
  }
}

export default function EventOrdering() {
  const [config, setConfig] = useState(null)
  const [channels, setChannels] = useState([])
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [triggering, setTriggering] = useState(false)
  const [status, setStatus] = useState(null)

  // Channel selection
  const [selectedChannelId, setSelectedChannelId] = useState('')
  const [channelOpen, setChannelOpen] = useState(false)
  const [streams, setStreams] = useState([])
  const [loadingStreams, setLoadingStreams] = useState(false)
  const [selectedStreamName, setSelectedStreamName] = useState('')

  // Regex builder state
  const [words, setWords] = useState([])
  const [wordData, setWordData] = useState({})
  const [selectedWords, setSelectedWords] = useState(new Set())
  const [generatedPattern, setGeneratedPattern] = useState('')

  // Test & preview
  const [testResults, setTestResults] = useState([])
  const [orderingPreview, setOrderingPreview] = useState([])

  // Overflow settings
  const [overflowChannelIds, setOverflowChannelIds] = useState([])
  const [overflowOpen, setOverflowOpen] = useState(false)
  const [returnAfterHours, setReturnAfterHours] = useState(6)
  const [overflowPreview, setOverflowPreview] = useState({ conflicts: [], staying: [], moving: [] })

  // Channel renaming
  const [channelRenamingEnabled, setChannelRenamingEnabled] = useState(false)
  const [channelNameTemplate, setChannelNameTemplate] = useState('')
  const [displayTimezone, setDisplayTimezone] = useState('')

  // Timezone
  const [channelTimezone, setChannelTimezone] = useState('')
  const [availableTimezones, setAvailableTimezones] = useState([])
  const [tzOpen, setTzOpen] = useState(false)
  const [displayTzOpen, setDisplayTzOpen] = useState(false)

  const { toast } = useToast()

  // Initialize timezones
  useEffect(() => {
    if (Intl?.supportedValuesOf) {
      setAvailableTimezones(Intl.supportedValuesOf('timeZone'))
    } else {
      setAvailableTimezones(['UTC', 'America/New_York', 'America/Chicago', 'America/Denver', 'America/Los_Angeles', 'Europe/London', 'Europe/Paris', 'Asia/Tokyo', 'Australia/Sydney'])
    }
  }, [])

  const loadData = useCallback(async () => {
    try {
      setLoading(true)
      const [configRes, channelsRes, statusRes] = await Promise.all([
        eventOrderingAPI.getConfig(),
        channelsAPI.getChannels(),
        eventOrderingAPI.getStatus(),
      ])
      setConfig(configRes.data)
      setChannels(channelsRes.data || [])
      setStatus(statusRes.data)
    } catch (err) {
      console.error('Failed to load data:', err)
      toast({ title: "Error", description: "Failed to load configuration", variant: "destructive" })
    } finally {
      setLoading(false)
    }
  }, [toast])

  useEffect(() => { loadData() }, [loadData])

  // Refresh status periodically
  useEffect(() => {
    const interval = setInterval(async () => {
      try { const res = await eventOrderingAPI.getStatus(); setStatus(res.data) } catch {}
    }, 10000)
    return () => clearInterval(interval)
  }, [])

  const getChannelColor = (channelId) => {
    const idx = channels.findIndex(c => String(c.id) === String(channelId))
    return CHANNEL_COLORS[((idx >= 0 ? idx : 0) % CHANNEL_COLORS.length)]
  }

  const getChannelName = (id) => {
    const ch = channels.find(c => String(c.id) === String(id))
    return ch?.name || `Channel ${id}`
  }

  const resolveNameTemplate = (template, streamName, channelId) => {
    if (!template || !streamName || !generatedPattern) return null
    try {
      const match = new RegExp(generatedPattern, 'i').exec(streamName)
      const groups = match?.groups || {}
      // Use the original name from config (not the potentially-renamed Dispatcharr name)
      const chId = channelId || selectedChannelId
      const baseName = config?.channels?.[chId]?.name || getChannelName(chId)
      const vars = { base_name: baseName, event_name: streamName, ...groups }
      // Compute event_time, event_date, timezone from parsed groups + display timezone
      const eventDate = parseGroupsToDate(groups)
      if (eventDate) {
        const tz = displayTimezone || channelTimezone || undefined
        const timeOpts = { hour: '2-digit', minute: '2-digit', hour12: false }
        const dateOpts = { year: 'numeric', month: '2-digit', day: '2-digit' }
        if (tz && tz.trim()) {
          timeOpts.timeZone = tz.trim()
          dateOpts.timeZone = tz.trim()
        }
        vars.event_time = eventDate.toLocaleTimeString('en-GB', timeOpts)
        vars.event_date = eventDate.toLocaleDateString('en-GB', dateOpts)
        // Timezone abbreviation
        try {
          const tzName = tz?.trim() || Intl.DateTimeFormat().resolvedOptions().timeZone
          const short = eventDate.toLocaleString('en-US', { timeZone: tzName, timeZoneName: 'short' })
          vars.timezone = short.split(' ').pop()
        } catch { vars.timezone = '' }
      }
      return template.replace(/\{(\w+)\}/g, (_, key) => vars[key] ?? `{${key}}`)
    } catch { return null }
  }

  // ==================== Channel Selection ====================
  const handleChannelChange = async (channelId) => {
    setSelectedChannelId(channelId)
    setSelectedStreamName('')
    setWords([])
    setWordData({})
    setGeneratedPattern('')
    setTestResults([])
    setOrderingPreview([])
    setOverflowChannelIds([])
    setReturnAfterHours(6)
    setChannelTimezone('')
    setChannelRenamingEnabled(false)
    setChannelNameTemplate('')
    setDisplayTimezone('')
    setOverflowPreview({ conflicts: [], staying: [], moving: [] })

    if (!channelId) { setStreams([]); return }

    // Load existing config for channel (before streams, so UI is populated even if stream fetch fails)
    if (config?.channels?.[channelId]) {
      const cc = config.channels[channelId]
      setGeneratedPattern(cc.pattern || '')
      setOverflowChannelIds(cc.overflow_channel_ids || [])
      setReturnAfterHours(cc.return_after_hours || 6)
      setChannelTimezone(cc.stream_timezone || '')
      setChannelRenamingEnabled(cc.channel_renaming_enabled ?? !!cc.channel_name_template)
      setChannelNameTemplate(cc.channel_name_template || '')
      setDisplayTimezone(cc.display_timezone || '')
    }

    try {
      setLoadingStreams(true)

      // Load streams via test-pattern with match-all
      const res = await eventOrderingAPI.testPattern({
        pattern: '.*',
        channel_id: parseInt(channelId),
      })
      const mainStreams = (res.data?.results || []).map((r, i) => ({
        name: r.stream_name,
        id: i,
        channelId: parseInt(channelId),
      }))

      // Load overflow streams if configured
      let allStreams = [...mainStreams]
      const cc = config?.channels?.[channelId]
      const tempOverflowIds = cc?.overflow_channel_ids || []

      if (tempOverflowIds.length > 0) {
        const overflowResults = await Promise.all(
          tempOverflowIds.map(async (oid) => {
            try {
              const r = await eventOrderingAPI.testPattern({ pattern: '.*', channel_id: parseInt(oid) })
              return (r.data?.results || []).map((s, i) => ({
                name: s.stream_name, id: i, channelId: parseInt(oid),
              }))
            } catch { return [] }
          })
        )
        overflowResults.forEach(arr => allStreams.push(...arr))
      }

      setStreams(allStreams)

      // Auto-select first stream
      if (mainStreams.length > 0) {
        setSelectedStreamName(mainStreams[0].name)
        parseSample(mainStreams[0].name)
      }

      // Run test if pattern exists and streams loaded
      if (cc?.pattern && allStreams.length > 0) {
        testPattern(cc.pattern, allStreams, cc.return_after_hours || 6, cc.overflow_channel_ids || [], cc.stream_timezone || '')
      } else {
        // No streams — ensure test results are cleared
        setTestResults([])
        setOrderingPreview([])
        setOverflowPreview({ conflicts: [], staying: [], moving: [] })
      }
    } catch (err) {
      console.error('Failed to load streams:', err)
      setStreams([])
      setTestResults([])
      setOrderingPreview([])
      setOverflowPreview({ conflicts: [], staying: [], moving: [] })
    } finally {
      setLoadingStreams(false)
    }
  }

  // ==================== Token Parsing ====================
  const parseSample = (sample) => {
    if (!sample) { setWords([]); setWordData({}); return }

    const tokens = sample.split(TOKEN_SPLIT_REGEX)
      .filter(t => t && t.length > 0)

    const newWords = []
    const newWordData = {}
    let idx = 0
    tokens.forEach((token) => {
      const id = `word-${idx}`
      if (/^\s+$/.test(token)) {
        newWords.push({ id, text: ' ', isSpace: true })
        newWordData[id] = { text: ' ', category: null, index: idx, isSpace: true }
      } else {
        newWords.push({ id, text: token.trim() })
        newWordData[id] = { text: token.trim(), category: null, index: idx }
      }
      idx++
    })

    setWords(newWords)
    setWordData(newWordData)
    setSelectedWords(new Set())
    setTestResults([])
  }

  // ==================== Drag & Drop ====================
  const handleDragStart = (e, wordId) => {
    const toMove = selectedWords.has(wordId) ? Array.from(selectedWords) : [wordId]
    e.dataTransfer.setData('text/plain', JSON.stringify(toMove))
    e.dataTransfer.effectAllowed = 'move'
  }

  const handleDragOver = (e) => { e.preventDefault(); e.dataTransfer.dropEffect = 'move' }

  const handleDrop = (e, category) => {
    e.preventDefault()
    try {
      const wordIds = JSON.parse(e.dataTransfer.getData('text/plain'))
      setWordData(prev => {
        const updated = { ...prev }
        wordIds.forEach(id => { if (updated[id]) updated[id] = { ...updated[id], category } })
        return updated
      })
      setSelectedWords(new Set())
    } catch {}
  }

  const handleWordClick = (e, wordId) => {
    if (e.shiftKey || e.ctrlKey || e.metaKey) {
      setSelectedWords(prev => {
        const s = new Set(prev)
        s.has(wordId) ? s.delete(wordId) : s.add(wordId)
        return s
      })
    } else {
      setSelectedWords(new Set([wordId]))
    }
  }

  const removeFromCategory = (wordId) => {
    setWordData(prev => ({ ...prev, [wordId]: { ...prev[wordId], category: null } }))
  }

  // ==================== Generate Regex ====================
  const generateRegex = () => {
    const allWords = Object.entries(wordData).sort((a, b) => a[1].index - b[1].index)
    const categorized = allWords.filter(([_, d]) => d.category && d.category !== 'ignore')

    if (categorized.length === 0) {
      toast({ title: "Error", description: "Please categorize at least one word", variant: "destructive" })
      return
    }

    // Merge consecutive tokens with the same category into groups
    // e.g. ["Chicago" team1, " " uncategorized, "Bulls" team1] → one merged group
    const merged = []
    let current = null

    allWords.forEach(([id, data]) => {
      if (data.category && data.category !== 'ignore') {
        if (current && current.category === data.category) {
          // Check if tokens between last categorized and this one are just spaces
          const betweenIndices = allWords.filter(([_, d]) =>
            d.index > current.lastIndex && d.index < data.index
          ).map(([_, d]) => d.text)
          const allSpaces = betweenIndices.every(t => /^\s+$/.test(t))

          if (allSpaces) {
            // Merge: extend current group
            current.texts.push(data.text)
            current.lastIndex = data.index
            current.combinedText = current.texts.join(' ')
            return
          }
        }

        // Start new group (flush current if exists)
        if (current) merged.push(current)
        current = {
          category: data.category,
          texts: [data.text],
          firstIndex: data.index,
          lastIndex: data.index,
          combinedText: data.text,
        }
      }
    })
    if (current) merged.push(current)

    // Build pattern from merged groups
    let pattern = ''
    let lastIdx = -1
    const usedCategories = {}

    merged.forEach((group) => {
      // Add separator between groups
      if (lastIdx >= 0 && group.firstIndex > lastIdx + 1) {
        const between = allWords.filter(([_, d]) => d.index > lastIdx && d.index < group.firstIndex).map(([_, d]) => d.text)
        const allDelims = between.every(t => (
          t.length === 1
          && (TOKEN_SEPARATOR_CHARS.includes(t) || t === '\\' || /\s/.test(t))
        ))
        if (allDelims && between.length > 0) {
          // Use \s* around punctuation to handle optional spaces (e.g. "07:" vs "04 :")
          const hasPunct = between.some(t => !/^\s+$/.test(t))
          pattern += between.map(t => t === ' ' ? (hasPunct ? '\\s*' : '\\s+') : escapeRegex(t)).join('')
        } else {
          pattern += '.*?'
        }
      }

      let groupName = group.category
      if (usedCategories[group.category]) {
        usedCategories[group.category]++
        groupName = `${group.category}_${usedCategories[group.category]}`
      } else {
        usedCategories[group.category] = 1
      }

      // Build capture pattern - for multi-word groups (like teams), use flexible multi-word match
      let capturePattern
      if (group.texts.length > 1 && (group.category === 'team1' || group.category === 'team2')) {
        // Flexible: match one or more words, allow alphanumeric (handles "76ers", variable-length names)
        capturePattern = '[A-Za-z0-9]+(?:\\s+[A-Za-z0-9]+)*'
      } else if (group.texts.length > 1) {
        const wordPattern = buildCapturePattern(group.texts[0], group.category)
        capturePattern = wordPattern + Array(group.texts.length - 1).fill(`\\s+${wordPattern}`).join('')
      } else {
        capturePattern = buildCapturePattern(group.combinedText, group.category)
      }

      pattern += `(?<${groupName}>${capturePattern})`
      lastIdx = group.lastIndex
    })

    // Add optional | prefix before first team group to handle streams like "NBA TV 1 | World @ Stars"
    if (pattern.includes('(?<team')) {
      pattern = pattern.replace(/(\(\?<team)/, '(?:.*?\\|\\s*)?$1')
    }

    setGeneratedPattern(pattern)
    testPattern(pattern, streams, returnAfterHours, overflowChannelIds)
  }

  // ==================== Test Pattern ====================
  const testPattern = async (pattern, streamList = null, grace = null, overflowIds = null, tz = null) => {
    const ss = streamList || streams
    const graceH = grace ?? returnAfterHours
    const oIds = overflowIds ?? overflowChannelIds
    const timezone = tz ?? channelTimezone
    if (!pattern || ss.length === 0) { setTestResults([]); setOrderingPreview([]); return }

    try {
      // Backend test results (uses parser if configured)
      const selectedParser = config?.channels?.[selectedChannelId]?.parser
      const testRes = await eventOrderingAPI.testPattern({
        pattern,
        channel_id: selectedChannelId ? parseInt(selectedChannelId) : undefined,
        timezone: channelTimezone,
        stream_names: ss.slice(0, 200).map(s => s.name),
        ...(selectedParser ? { parser: selectedParser } : {}),
      })
      const results = (testRes.data?.results || []).map((r, idx) => ({
        name: r.stream_name,
        matched: !!r.matched,
        groups: r.groups || null,
        channelId: ss[idx]?.channelId,
      }))
      setTestResults(results)

      const regex = new RegExp(pattern, 'i')

      // Ordering preview
      const now = new Date()
      const parsed = ss.map((s, idx) => {
        const match = s.name.match(regex)
        let eventTime = null
        let orderNum = 999
        if (match?.groups) {
          if (match.groups.order) orderNum = parseInt(match.groups.order) || 999
          eventTime = parseGroupsToDate(match.groups, timezone || null)
        }
        return { name: s.name, eventTime, orderNum, originalIndex: idx, channelId: s.channelId }
      })

      const upcoming = []
      const past = []
      parsed.forEach(s => {
        if (s.eventTime) {
          const hoursDiff = (now - s.eventTime) / (1000 * 60 * 60)
          if (hoursDiff < graceH) upcoming.push(s)
          else past.push(s)
        } else {
          past.push(s)
        }
      })

      upcoming.sort((a, b) => {
        if (a.eventTime && b.eventTime) { const d = a.eventTime - b.eventTime; if (d !== 0) return d }
        return a.orderNum - b.orderNum
      })
      past.sort((a, b) => {
        if (a.eventTime && b.eventTime) { const d = b.eventTime - a.eventTime; if (d !== 0) return d }
        return a.orderNum - b.orderNum
      })

      const ordered = [...upcoming, ...past].map((s, i) => ({
        ...s, newIndex: i + 1, isUpcoming: upcoming.includes(s)
      }))
      setOrderingPreview(ordered)
      calculateOverflowPreview(parsed, oIds)
    } catch (err) {
      toast({ title: "Pattern Error", description: err.message, variant: "destructive" })
      setTestResults([])
      setOrderingPreview([])
    }
  }

  // Re-run test when returnAfterHours changes
  useEffect(() => {
    if (generatedPattern && streams.length > 0) {
      testPattern(generatedPattern, streams, returnAfterHours, overflowChannelIds)
    }
  }, [returnAfterHours])

  // ==================== Overflow Preview ====================
  const calculateOverflowPreview = (parsed, currentOverflowIds) => {
    const events = {}
    parsed.forEach(s => {
      const o = s.orderNum
      if (!events[o]) events[o] = []
      events[o].push(s)
    })

    const timeSlots = {}
    Object.entries(events).forEach(([orderNum, eventStreams]) => {
      const et = eventStreams[0].eventTime
      if (et) {
        const key = et.toISOString().slice(0, 16)
        if (!timeSlots[key]) timeSlots[key] = []
        timeSlots[key].push({ orderNum: parseInt(orderNum), streams: eventStreams, eventTime: et })
      }
    })

    const conflicts = [], staying = [], moving = []
    Object.entries(timeSlots).forEach(([key, eventsAtTime]) => {
      if (eventsAtTime.length > 1) {
        eventsAtTime.sort((a, b) => a.orderNum - b.orderNum)
        const stayColor = getChannelColor(selectedChannelId)
        staying.push({
          orderNum: eventsAtTime[0].orderNum, streamCount: eventsAtTime[0].streams.length,
          streamNames: eventsAtTime[0].streams.map(s => s.name), eventTime: eventsAtTime[0].eventTime,
          channelName: getChannelName(selectedChannelId), channelId: selectedChannelId, color: stayColor,
        })
        eventsAtTime.slice(1).forEach((ev, idx) => {
          if (!currentOverflowIds?.length) return
          const oid = currentOverflowIds[idx % currentOverflowIds.length]
          const color = getChannelColor(oid)
          moving.push({
            orderNum: ev.orderNum, streamCount: ev.streams.length,
            streamNames: ev.streams.map(s => s.name), eventTime: ev.eventTime,
            channelName: getChannelName(oid), channelId: oid, color,
          })
        })
        conflicts.push({ timeKey: key, eventTime: eventsAtTime[0].eventTime, events: eventsAtTime.map(e => ({ orderNum: e.orderNum, streamCount: e.streams.length })) })
      }
    })
    setOverflowPreview({ conflicts, staying, moving })
  }

  // Recalculate overflow when IDs change
  useEffect(() => {
    if (testResults.length > 0 && generatedPattern) {
      testPattern(generatedPattern, streams, returnAfterHours, overflowChannelIds)
    }
  }, [overflowChannelIds])

  // ==================== Save / Delete ====================
  const handleSave = async () => {
    if (!selectedChannelId || !generatedPattern) return
    try {
      setSaving(true)
      const ch = channels.find(c => c.id === parseInt(selectedChannelId))
      // Preserve the original channel name from config (not the potentially-renamed Dispatcharr name)
      const existingName = config?.channels?.[selectedChannelId]?.name
      const updated = {
        ...config,
        channels: {
          ...config.channels,
          [selectedChannelId]: {
            pattern: generatedPattern,
            name: existingName || ch?.name || `Channel ${selectedChannelId}`,
            overflow_channel_ids: overflowChannelIds,
            return_after_hours: returnAfterHours,
            stream_timezone: channelTimezone,
            channel_renaming_enabled: channelRenamingEnabled,
            ...(channelNameTemplate ? { channel_name_template: channelNameTemplate } : {}),
            ...(displayTimezone ? { display_timezone: displayTimezone } : {}),
          }
        }
      }
      const res = await eventOrderingAPI.updateConfig(updated)
      setConfig(res.data.config)
      toast({ title: "Saved", description: `Pattern saved for ${ch?.name || 'channel'}` })
    } catch {
      toast({ title: "Error", description: "Failed to save", variant: "destructive" })
    } finally {
      setSaving(false)
    }
  }

  const handleDeleteChannel = async (channelId) => {
    try {
      const updated = { ...config, channels: { ...config.channels } }
      delete updated.channels[channelId]
      const res = await eventOrderingAPI.updateConfig(updated)
      setConfig(res.data.config)
      if (selectedChannelId === channelId) setSelectedChannelId('')
      toast({ title: "Deleted", description: "Channel configuration removed" })
    } catch {
      toast({ title: "Error", description: "Failed to delete", variant: "destructive" })
    }
  }

  // ==================== Global actions ====================
  const handleToggleEnabled = async (enabled) => {
    try {
      setSaving(true)
      const res = await eventOrderingAPI.updateConfig({ ...config, enabled })
      setConfig(res.data.config)
      const statusRes = await eventOrderingAPI.getStatus()
      setStatus(statusRes.data)
      toast({ title: enabled ? "Enabled" : "Disabled", description: `Event ordering ${enabled ? 'enabled' : 'disabled'}` })
    } catch {
      toast({ title: "Error", description: "Failed to update", variant: "destructive" })
    } finally {
      setSaving(false)
    }
  }

  const handleUpdateFrequency = async (val) => {
    const freq = parseInt(val)
    if (isNaN(freq) || freq < 30) return
    try {
      await eventOrderingAPI.updateConfig({ ...config, frequency: freq })
      setConfig(prev => ({ ...prev, frequency: freq }))
    } catch {}
  }

  const handleTrigger = async () => {
    try {
      setTriggering(true)
      const res = await eventOrderingAPI.trigger()
      toast({ title: "Complete", description: `Processed ${res.data.channels_processed || 0} channels` })
      const statusRes = await eventOrderingAPI.getStatus()
      setStatus(statusRes.data)
    } catch {
      toast({ title: "Error", description: "Failed to trigger", variant: "destructive" })
    } finally {
      setTriggering(false)
    }
  }

  const toggleOverflowChannel = (id) => {
    setOverflowChannelIds(prev => prev.includes(id) ? prev.filter(x => x !== id) : [...prev, id])
  }

  // Available words (not categorized, not spaces)
  const availableWords = words.filter(w => !wordData[w.id]?.category && !wordData[w.id]?.isSpace)
  const getCategoryWords = (catId) => words.filter(w => wordData[w.id]?.category === catId)

  if (loading) {
    return <div className="flex items-center justify-center h-64"><Loader2 className="h-8 w-8 animate-spin text-muted-foreground" /></div>
  }

  const configuredChannels = config?.channels || {}

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Event Ordering</h1>
          <p className="text-muted-foreground">Automatically reorder streams by event start times</p>
        </div>
        <div className="flex items-center gap-4">
          <div className="flex items-center gap-2">
            <Label htmlFor="eo-enabled">Enabled</Label>
            <Switch id="eo-enabled" checked={config?.enabled || false} onCheckedChange={handleToggleEnabled} disabled={saving} />
          </div>
        </div>
      </div>

      <Tabs defaultValue="configuration">
        <TabsList>
          <TabsTrigger value="configuration">Configuration</TabsTrigger>
          <TabsTrigger value="status">Status</TabsTrigger>
        </TabsList>

        {/* ========== Configuration Tab ========== */}
        <TabsContent value="configuration" className="space-y-4">

          {/* Global Settings */}
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2"><Settings className="h-5 w-5" />Global Settings</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="w-64 space-y-2">
                <Label htmlFor="freq">Check Interval (seconds)</Label>
                <Input id="freq" type="number" min={30} value={config?.frequency || 300}
                  onChange={e => setConfig(p => ({ ...p, frequency: parseInt(e.target.value) || 300 }))}
                  onBlur={e => handleUpdateFrequency(e.target.value)}
                />
                <p className="text-xs text-muted-foreground">How often the processor checks and reorders (min 30s)</p>
              </div>
            </CardContent>
          </Card>

          {/* Configured Channels (chips) */}
          {Object.keys(configuredChannels).length > 0 && (
            <Card>
              <CardHeader><CardTitle>Configured Channels</CardTitle></CardHeader>
              <CardContent>
                <div className="flex flex-wrap gap-2">
                  {Object.entries(configuredChannels).map(([chId, cc]) => (
                    <Badge key={chId}
                      className={cn("cursor-pointer text-sm py-1 px-3", selectedChannelId === chId ? "bg-primary text-primary-foreground" : "bg-secondary text-secondary-foreground hover:bg-secondary/80")}
                      onClick={() => handleChannelChange(chId)}
                    >
                      {cc.name}
                      <button className="ml-2 hover:text-destructive" onClick={(e) => { e.stopPropagation(); handleDeleteChannel(chId) }}>
                        <X className="h-3 w-3" />
                      </button>
                    </Badge>
                  ))}
                </div>
              </CardContent>
            </Card>
          )}

          {/* Step 1: Channel + Step 2: Stream Selection */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <Card>
              <CardHeader><CardTitle>Step 1: Select Channel</CardTitle></CardHeader>
              <CardContent>
                <Popover open={channelOpen} onOpenChange={setChannelOpen}>
                  <PopoverTrigger asChild>
                    <Button variant="outline" role="combobox" className="w-full justify-between">
                      {selectedChannelId ? getChannelName(selectedChannelId) : "Select a channel..."}
                      <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
                    </Button>
                  </PopoverTrigger>
                  <PopoverContent className="w-full p-0" align="start">
                    <Command>
                      <CommandInput placeholder="Search channels..." />
                      <CommandList>
                        <CommandEmpty>No channels found.</CommandEmpty>
                        <CommandGroup>
                          {channels.map(ch => (
                            <CommandItem key={ch.id} value={ch.name} onSelect={() => { handleChannelChange(String(ch.id)); setChannelOpen(false) }}>
                              <Check className={cn("mr-2 h-4 w-4", String(ch.id) === selectedChannelId ? "opacity-100" : "opacity-0")} />
                              {ch.name} (ID: {ch.id})
                            </CommandItem>
                          ))}
                        </CommandGroup>
                      </CommandList>
                    </Command>
                  </PopoverContent>
                </Popover>
              </CardContent>
            </Card>

            <Card>
              <CardHeader><CardTitle>Step 2: Select Sample Stream</CardTitle></CardHeader>
              <CardContent>
                {loadingStreams ? (
                  <div className="flex justify-center p-4"><Loader2 className="h-5 w-5 animate-spin" /></div>
                ) : (
                  <Select value={selectedStreamName} onValueChange={v => { setSelectedStreamName(v); parseSample(v) }} disabled={!selectedChannelId}>
                    <SelectTrigger><SelectValue placeholder="Select a stream..." /></SelectTrigger>
                    <SelectContent>
                      {streams.map((s, i) => (
                        <SelectItem key={i} value={s.name}>{s.name}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                )}
                {streams.length === 0 && selectedChannelId && !loadingStreams && (
                  <p className="text-sm text-muted-foreground mt-2">No streams found in this channel</p>
                )}
              </CardContent>
            </Card>
          </div>

          {/* Step 3: Token Categorization (drag & drop) */}
          {words.length > 0 && (
            <Card>
              <CardHeader>
                <CardTitle>Step 3: Categorize Time Components</CardTitle>
                <CardDescription>Drag words to their categories. Shift/Ctrl+click to select multiple. Only categorize the start time components.</CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                {/* Available Words */}
                <div className="border rounded-lg p-3 min-h-[48px] bg-muted/30">
                  <p className="text-xs font-medium text-muted-foreground mb-2">Available Words</p>
                  <div className="flex flex-wrap gap-1.5">
                    {availableWords.length === 0 ? (
                      <span className="text-sm text-muted-foreground">All words categorized</span>
                    ) : availableWords.map(w => (
                      <span key={w.id} draggable onDragStart={e => handleDragStart(e, w.id)} onClick={e => handleWordClick(e, w.id)}
                        className={cn("inline-flex items-center px-2.5 py-1 rounded-md text-sm font-medium cursor-grab active:cursor-grabbing border select-none",
                          selectedWords.has(w.id) ? "bg-primary text-primary-foreground border-primary" : "bg-background border-border hover:bg-accent"
                        )}>
                        {w.text}
                      </span>
                    ))}
                  </div>
                </div>

                {/* Drop Zones */}
                <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-5 gap-2">
                  {CATEGORIES.map(cat => (
                    <div key={cat.id}
                      onDragOver={handleDragOver}
                      onDrop={e => handleDrop(e, cat.id)}
                      className="border-2 border-dashed rounded-lg p-2 min-h-[72px] transition-colors hover:opacity-80"
                      style={{ borderColor: cat.color, backgroundColor: `${cat.color}10` }}
                    >
                      <p className="text-xs font-bold mb-1" style={{ color: cat.color }}
                        title={cat.tooltip}>{cat.label}</p>
                      <div className="flex flex-wrap gap-1">
                        {getCategoryWords(cat.id).map(w => (
                          <span key={w.id} className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs text-white font-medium"
                            style={{ backgroundColor: cat.color }}>
                            {w.text}
                            <button onClick={() => removeFromCategory(w.id)} className="hover:opacity-70"><X className="h-3 w-3" /></button>
                          </span>
                        ))}
                      </div>
                    </div>
                  ))}
                </div>

                <div className="flex items-center gap-4 pt-2">
                  <Button onClick={generateRegex}><RefreshCw className="h-4 w-4 mr-2" />Generate Pattern</Button>

                  {/* Timezone */}
                  <div className="flex items-center gap-2 flex-1 max-w-md">
                    <Label className="shrink-0 text-sm">Timezone:</Label>
                    <Popover open={tzOpen} onOpenChange={setTzOpen}>
                      <PopoverTrigger asChild>
                        <Button variant="outline" className="h-9 justify-between flex-1">
                          {channelTimezone || 'Auto / Local'}
                          <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
                        </Button>
                      </PopoverTrigger>
                      <PopoverContent className="w-[300px] p-0" align="start">
                        <Command>
                          <CommandInput placeholder="Search timezone..." />
                          <CommandList>
                            <CommandEmpty>No timezone found.</CommandEmpty>
                            <CommandGroup>
                              <CommandItem value="auto-local" onSelect={() => { setChannelTimezone(''); setTzOpen(false) }}>
                                <Check className={cn("mr-2 h-4 w-4", !channelTimezone ? "opacity-100" : "opacity-0")} />
                                Auto / Local
                              </CommandItem>
                              {availableTimezones.map(tz => (
                                <CommandItem key={tz} value={tz} onSelect={() => { setChannelTimezone(tz); setTzOpen(false) }}>
                                  <Check className={cn("mr-2 h-4 w-4", channelTimezone === tz ? "opacity-100" : "opacity-0")} />
                                  {tz}
                                </CommandItem>
                              ))}
                            </CommandGroup>
                          </CommandList>
                        </Command>
                      </PopoverContent>
                    </Popover>
                  </div>
                </div>
              </CardContent>
            </Card>
          )}

          {/* Step 4: Generated Pattern */}
          {(generatedPattern || selectedChannelId) && (
            <Card>
              <CardHeader><CardTitle>Step 4: Pattern</CardTitle></CardHeader>
              <CardContent className="space-y-3">
                {generatedPattern && (
                  <div className="flex items-start gap-2 bg-zinc-900 text-green-400 font-mono text-sm p-3 rounded-lg">
                    <code className="flex-1 break-all">{generatedPattern}</code>
                    <Button variant="ghost" size="icon" className="shrink-0 text-zinc-500 hover:text-white h-6 w-6"
                      onClick={() => { navigator.clipboard.writeText(generatedPattern); toast({ title: "Copied" }) }}>
                      <Copy className="h-4 w-4" />
                    </Button>
                  </div>
                )}
                <div className="space-y-2">
                  <Label className="text-sm">Manual Pattern Entry</Label>
                  <Input value={generatedPattern} onChange={e => setGeneratedPattern(e.target.value)} className="font-mono text-sm"
                    placeholder="(?<hour>\d{1,2}):(?<minute>\d{2})(?<ampm>[AaPp][Mm])" />
                  <p className="text-xs text-muted-foreground">Named groups: {(() => {
                    const p = generatedPattern; if (!p) return 'year, month, day, hour, minute, second, ampm, order, league, team1, team2'
                    const groups = []; const re = /\(\?<(\w+)>/g; let m
                    while ((m = re.exec(p)) !== null) groups.push(m[1])
                    return groups.length > 0 ? groups.join(', ') : 'none detected'
                  })()}</p>
                </div>
                <div className="flex gap-2">
                  <Button variant="outline" onClick={() => testPattern(generatedPattern)} disabled={!generatedPattern}>
                    <TestTube className="h-4 w-4 mr-2" />Test Pattern
                  </Button>
                  <Button onClick={handleSave} disabled={saving || !generatedPattern || !selectedChannelId}>
                    {saving ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Save className="h-4 w-4 mr-2" />}
                    Save Pattern
                  </Button>
                </div>
              </CardContent>
            </Card>
          )}

          {/* Test Results */}
          {testResults.length > 0 && (
            <Card>
              <CardHeader>
                <CardTitle>Test Results</CardTitle>
                <CardDescription>
                  {testResults.filter(r => r.matched).length} matched, {testResults.filter(r => !r.matched).length} unmatched
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="max-h-[400px] overflow-auto space-y-1.5">
                  {testResults.map((r, i) => (
                    <div key={i} className={cn("p-2 rounded border text-sm font-mono", r.matched ? "border-green-600/40 bg-green-950/20" : "border-red-600/40 bg-red-950/20")}>
                      <div className={cn("break-all", r.matched ? "text-green-400" : "text-red-400")}>
                        {r.matched ? '✓' : '✗'} {r.name}
                      </div>
                      {r.groups && (
                        <div className="flex flex-wrap gap-1 mt-1">
                          {Object.entries(r.groups).filter(([_, v]) => v).map(([k, v]) => (
                            <Badge key={k} variant="outline" className="text-xs font-mono">{k}: {v}</Badge>
                          ))}
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              </CardContent>
            </Card>
          )}

          {/* Ordering Preview */}
          {orderingPreview.length > 0 && (
            <Card>
              <CardHeader>
                <CardTitle>Ordering Preview</CardTitle>
                <CardDescription>How streams will be reordered after applying the pattern</CardDescription>
                {channelNameTemplate && (() => {
                  const topUpcoming = orderingPreview.find(item => item.isUpcoming)
                  const topStream = topUpcoming || orderingPreview[0]
                  const resolved = topStream ? resolveNameTemplate(channelNameTemplate, topStream.name, selectedChannelId) : null
                  return resolved ? (
                    <div className="mt-2 flex items-center gap-2">
                      <span className="text-xs text-muted-foreground">Channel name:</span>
                      <span className="text-sm font-semibold bg-primary/10 text-primary px-2 py-0.5 rounded">
                        {resolved}
                      </span>
                    </div>
                  ) : null
                })()}
              </CardHeader>
              <CardContent>
                <div className="max-h-[400px] overflow-auto">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Name</TableHead>
                        <TableHead className="w-[120px] text-center">Channel</TableHead>
                        <TableHead className="w-[70px] text-center">New</TableHead>
                        <TableHead className="w-[70px] text-center">Change</TableHead>
                        <TableHead className="w-[70px] text-center">Old</TableHead>
                        <TableHead className="w-[100px] text-center">Time</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {orderingPreview.map((item, i) => {
                        const oldPos = item.originalIndex + 1
                        const change = oldPos - item.newIndex
                        const chColor = getChannelColor(item.channelId || selectedChannelId)
                        return (
                          <TableRow key={i} className={item.isUpcoming ? 'bg-green-950/10' : ''}>
                            <TableCell className="font-mono text-xs max-w-[300px] truncate">{item.name}</TableCell>
                            <TableCell className="text-center">
                              <span className="inline-block text-xs px-2 py-0.5 rounded font-medium"
                                style={{ backgroundColor: chColor.bg, color: chColor.text, border: `1px solid ${chColor.border}` }}>
                                {getChannelName(item.channelId || selectedChannelId)}
                              </span>
                            </TableCell>
                            <TableCell className="text-center font-bold">{item.newIndex}</TableCell>
                            <TableCell className="text-center">
                              {change > 0 ? <span className="text-green-500">↑{change}</span>
                                : change < 0 ? <span className="text-red-500">↓{Math.abs(change)}</span>
                                : <span className="text-muted-foreground">—</span>}
                            </TableCell>
                            <TableCell className="text-center text-muted-foreground">{oldPos}</TableCell>
                            <TableCell className="text-center text-xs">{formatCountdown(item.eventTime)}</TableCell>
                          </TableRow>
                        )
                      })}
                    </TableBody>
                  </Table>
                </div>
              </CardContent>
            </Card>
          )}

          {/* Overflow Settings */}
          {selectedChannelId && (
            <Card>
              <CardHeader>
                <CardTitle>Overflow Settings</CardTitle>
                <CardDescription>When two events share the same time slot, move one to an overflow channel</CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <Label>Overflow Channels</Label>
                    <Popover open={overflowOpen} onOpenChange={setOverflowOpen}>
                      <PopoverTrigger asChild>
                        <Button variant="outline" className="w-full justify-between">
                          {overflowChannelIds.length > 0 ? `${overflowChannelIds.length} selected` : "Select overflow channels..."}
                          <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
                        </Button>
                      </PopoverTrigger>
                      <PopoverContent className="w-full p-0" align="start">
                        <Command>
                          <CommandInput placeholder="Search..." />
                          <CommandList>
                            <CommandEmpty>No channels.</CommandEmpty>
                            <CommandGroup>
                              {channels.filter(c => String(c.id) !== selectedChannelId).map(ch => (
                                <CommandItem key={ch.id} value={ch.name} onSelect={() => toggleOverflowChannel(ch.id)}>
                                  <Check className={cn("mr-2 h-4 w-4", overflowChannelIds.includes(ch.id) ? "opacity-100" : "opacity-0")} />
                                  {ch.name}
                                </CommandItem>
                              ))}
                            </CommandGroup>
                          </CommandList>
                        </Command>
                      </PopoverContent>
                    </Popover>
                    {overflowChannelIds.length > 0 && (
                      <div className="flex flex-wrap gap-1">
                        {overflowChannelIds.map(id => {
                          const c = getChannelColor(id)
                          return (
                            <span key={id} className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium cursor-pointer"
                              style={{ backgroundColor: c.bg, color: c.text, border: `1px solid ${c.border}` }}
                              onClick={() => toggleOverflowChannel(id)}>
                              {getChannelName(id)} <X className="h-3 w-3" />
                            </span>
                          )
                        })}
                      </div>
                    )}
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="return-h">Return After (hours)</Label>
                    <Input id="return-h" type="number" min={1} max={48} value={returnAfterHours}
                      onChange={e => setReturnAfterHours(parseInt(e.target.value) || 6)} />
                    <p className="text-xs text-muted-foreground">Streams return to main channel after this many hours</p>
                  </div>
                </div>
                {/* Channel Renaming - Drag & Drop Template Builder */}
                <div className="border-t pt-4 space-y-3">
                  <div className="flex items-center justify-between">
                    <div>
                      <Label className="text-base font-semibold">Channel Renaming</Label>
                      <p className="text-xs text-muted-foreground mt-0.5">
                        Rename channel based on current event using a template.
                      </p>
                    </div>
                    <Switch checked={channelRenamingEnabled} onCheckedChange={setChannelRenamingEnabled} />
                  </div>

                  {channelRenamingEnabled && <>
                  {/* Available variables - built from regex groups + builtins */}
                  <div className="border rounded-lg p-3 bg-muted/30">
                    <p className="text-xs font-medium text-muted-foreground mb-2">Available Variables</p>
                    <div className="flex flex-wrap gap-1.5">
                      {(() => {
                        // Builtins always available
                        const builtins = [
                          { id: 'base_name', label: 'Base Name', color: '#3b82f6' },
                          { id: 'event_name', label: 'Event Name', color: '#8b5cf6' },
                          { id: 'event_time', label: 'Event Time', color: '#10b981' },
                          { id: 'event_date', label: 'Event Date', color: '#06b6d4' },
                          { id: 'timezone', label: 'Timezone', color: '#6366f1' },
                        ]
                        // Extract named groups from current pattern
                        const groupNames = []
                        if (generatedPattern) {
                          const re = /\(\?<(\w+)>/g
                          let m
                          while ((m = re.exec(generatedPattern)) !== null) groupNames.push(m[1])
                        }
                        // Map group names to categories for color
                        const fromPattern = groupNames.map(name => {
                          const cat = CATEGORIES.find(c => c.id === name || name.startsWith(c.id))
                          return { id: name, label: cat?.label || name, color: cat?.color || '#6b7280' }
                        })
                        return [...builtins, ...fromPattern]
                      })().map(v => (
                        <span key={v.id} draggable
                          onDragStart={e => { e.dataTransfer.setData('text/plain', `{${v.id}}`); e.dataTransfer.effectAllowed = 'copy' }}
                          className="inline-flex items-center px-2.5 py-1 rounded-md text-xs font-medium cursor-grab active:cursor-grabbing text-white select-none"
                          style={{ backgroundColor: v.color }}
                          title={v.id}>
                          {v.label}
                        </span>
                      ))}
                    </div>
                  </div>

                  {/* Template drop zone + editable input */}
                  <div
                    onDragOver={e => { e.preventDefault(); e.dataTransfer.dropEffect = 'copy' }}
                    onDrop={e => {
                      e.preventDefault()
                      const variable = e.dataTransfer.getData('text/plain')
                      if (variable.startsWith('{') && variable.endsWith('}')) {
                        setChannelNameTemplate(prev => (prev ? prev + variable : variable))
                      }
                    }}
                    className="border-2 border-dashed rounded-lg p-3 min-h-[56px] transition-colors hover:border-primary/50"
                    style={{ borderColor: channelNameTemplate ? '#3b82f6' : undefined }}
                  >
                    <p className="text-xs font-medium text-muted-foreground mb-1.5">Template (drop here or type)</p>
                    <div className="flex items-center gap-1.5">
                      <Input
                        className="border-0 p-0 h-auto text-sm font-mono focus-visible:ring-0 bg-transparent"
                        placeholder="Drop variables or type: {base_name} - {league} {hour}:{minute}{ampm}"
                        value={channelNameTemplate}
                        onChange={e => setChannelNameTemplate(e.target.value)}
                      />
                      {channelNameTemplate && (
                        <button onClick={() => setChannelNameTemplate('')}
                          className="shrink-0 text-muted-foreground hover:text-foreground">
                          <X className="h-4 w-4" />
                        </button>
                      )}
                    </div>
                  </div>

                  {/* Display timezone for {event_time} and {timezone} */}
                  {channelNameTemplate && (channelNameTemplate.includes('{event_time}') || channelNameTemplate.includes('{timezone}') || channelNameTemplate.includes('{event_date}')) && (
                    <div className="flex items-center gap-2">
                      <Label className="shrink-0 text-sm">Display timezone:</Label>
                      <Popover open={displayTzOpen} onOpenChange={setDisplayTzOpen}>
                        <PopoverTrigger asChild>
                          <Button variant="outline" className="h-9 max-w-[300px] justify-between">
                            {displayTimezone || `Same as source (${channelTimezone || 'local'})`}
                            <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
                          </Button>
                        </PopoverTrigger>
                        <PopoverContent className="w-[300px] p-0" align="start">
                          <Command>
                            <CommandInput placeholder="Search timezone..." />
                            <CommandList>
                              <CommandEmpty>No timezone found.</CommandEmpty>
                              <CommandGroup>
                                <CommandItem value="same-as-source" onSelect={() => { setDisplayTimezone(''); setDisplayTzOpen(false) }}>
                                  <Check className={cn("mr-2 h-4 w-4", !displayTimezone ? "opacity-100" : "opacity-0")} />
                                  Same as source ({channelTimezone || 'local'})
                                </CommandItem>
                                {availableTimezones.map(tz => (
                                  <CommandItem key={tz} value={tz} onSelect={() => { setDisplayTimezone(tz); setDisplayTzOpen(false) }}>
                                    <Check className={cn("mr-2 h-4 w-4", displayTimezone === tz ? "opacity-100" : "opacity-0")} />
                                    {tz}
                                  </CommandItem>
                                ))}
                              </CommandGroup>
                            </CommandList>
                          </Command>
                        </PopoverContent>
                      </Popover>
                    </div>
                  )}

                  {/* Live preview */}
                  {channelNameTemplate && (
                    <div className="rounded-lg border p-3 space-y-1">
                      <p className="text-xs font-medium text-muted-foreground">Preview</p>
                      <p className="text-sm font-semibold">
                        {selectedStreamName
                          ? (resolveNameTemplate(channelNameTemplate, selectedStreamName, selectedChannelId)
                            || channelNameTemplate)
                          : channelNameTemplate.replace(/\{base_name\}/g,
                              config?.channels?.[selectedChannelId]?.name || getChannelName(selectedChannelId)
                            ).replace(/\{(\w+)\}/g, (_, k) => `[${k}]`)}
                      </p>
                      {selectedStreamName && (
                        <p className="text-xs text-muted-foreground">Based on: <span className="font-mono">{selectedStreamName}</span></p>
                      )}
                    </div>
                  )}
                  </>}
                </div>

                <Button onClick={handleSave} disabled={saving || !generatedPattern || !selectedChannelId}>
                  {saving ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Save className="h-4 w-4 mr-2" />}
                  Save Configuration
                </Button>
              </CardContent>
            </Card>
          )}

          {/* Overflow Preview */}
          {overflowChannelIds.length > 0 && overflowPreview.conflicts.length > 0 && (
            <Card>
              <CardHeader>
                <CardTitle>Overflow Preview</CardTitle>
                <CardDescription>
                  {overflowPreview.conflicts.length} time conflict(s) detected.
                  {' '}{overflowPreview.moving.length} event(s) ({overflowPreview.moving.reduce((s, e) => s + e.streamCount, 0)} streams) will be moved.
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-3">
                {overflowPreview.conflicts.map((conflict, idx) => (
                  <div key={idx} className="border rounded-lg p-3 space-y-2">
                    <p className="text-sm font-semibold">
                      Conflict at {conflict.eventTime.toLocaleString('en-US', { year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', hour12: false })}
                    </p>
                    <div className="flex gap-2 flex-wrap">
                      {overflowPreview.staying.filter(s => s.eventTime.getTime() === conflict.eventTime.getTime()).map((ev, i) => {
                        const resolved = channelNameTemplate ? resolveNameTemplate(channelNameTemplate, ev.streamNames[0], ev.channelId) : null
                        return (
                          <div key={`stay-${i}`} className="flex-1 min-w-[200px] rounded-lg p-2.5"
                            style={{ backgroundColor: ev.color.bg, border: `2px solid ${ev.color.border}` }}>
                            <p className="text-xs font-bold" style={{ color: ev.color.text }}>{ev.channelName}</p>
                            {resolved && <p className="text-xs mt-0.5" style={{ color: ev.color.text, opacity: 0.8 }}>→ {resolved}</p>}
                            <p className="text-xs font-mono mt-1" style={{ color: ev.color.text }}>{ev.streamNames[0]}</p>
                            {ev.streamCount > 1 && <p className="text-xs mt-0.5" style={{ color: ev.color.text, opacity: 0.7 }}>+ {ev.streamCount - 1} backup(s)</p>}
                          </div>
                        )
                      })}
                      {overflowPreview.moving.filter(m => m.eventTime.getTime() === conflict.eventTime.getTime()).map((ev, i) => {
                        const resolved = channelNameTemplate ? resolveNameTemplate(channelNameTemplate, ev.streamNames[0], ev.channelId) : null
                        return (
                          <div key={`move-${i}`} className="flex-1 min-w-[200px] rounded-lg p-2.5"
                            style={{ backgroundColor: ev.color.bg, border: `2px solid ${ev.color.border}` }}>
                            <p className="text-xs font-bold" style={{ color: ev.color.text }}>→ {ev.channelName}</p>
                            {resolved && <p className="text-xs mt-0.5" style={{ color: ev.color.text, opacity: 0.8 }}>→ {resolved}</p>}
                            <p className="text-xs font-mono mt-1" style={{ color: ev.color.text }}>{ev.streamNames[0]}</p>
                            {ev.streamCount > 1 && <p className="text-xs mt-0.5" style={{ color: ev.color.text, opacity: 0.7 }}>+ {ev.streamCount - 1} backup(s)</p>}
                          </div>
                        )
                      })}
                    </div>
                  </div>
                ))}
                <Alert><AlertDescription>Streams will return to this channel after {returnAfterHours} hour(s).</AlertDescription></Alert>
              </CardContent>
            </Card>
          )}

          {/* No conflicts message */}
          {overflowChannelIds.length > 0 && orderingPreview.length > 0 && overflowPreview.conflicts.length === 0 && (
            <Alert><CheckCircle2 className="h-4 w-4" /><AlertDescription>No time conflicts detected. All events are at different times.</AlertDescription></Alert>
          )}
        </TabsContent>

        {/* ========== Status Tab ========== */}
        <TabsContent value="status" className="space-y-4">
          <Card>
            <CardHeader><CardTitle className="flex items-center gap-2"><Zap className="h-5 w-5" />Processor Status</CardTitle></CardHeader>
            <CardContent className="space-y-4">
              <div className="flex items-center gap-4">
                {status?.processor_running ? (
                  <Badge variant="default" className="bg-green-600"><CheckCircle2 className="h-3 w-3 mr-1" />Running</Badge>
                ) : (
                  <Badge variant="secondary"><XCircle className="h-3 w-3 mr-1" />Stopped</Badge>
                )}
                <Button size="sm" variant="outline" onClick={handleTrigger} disabled={triggering}>
                  {triggering ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Play className="h-4 w-4 mr-2" />}Trigger Now
                </Button>
                <Button size="sm" variant="outline" onClick={loadData}><RefreshCw className="h-4 w-4 mr-2" />Refresh</Button>
              </div>
              {status?.last_run?.timestamp && (
                <div className="space-y-2">
                  <p className="text-sm text-muted-foreground"><Clock className="h-4 w-4 inline mr-1" />Last run: {new Date(status.last_run.timestamp).toLocaleString()}</p>
                  <p className="text-sm font-medium">Channels processed: {status.last_run.channels_processed || 0}</p>
                  {status.last_run.results && (
                    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-2">
                      {Object.entries(status.last_run.results).map(([chId, result]) => (
                        <Card key={chId} className="p-3">
                          <p className="text-sm font-medium">{configuredChannels[chId]?.name || `Channel ${chId}`}</p>
                          {result.success ? (
                            <p className="text-xs text-muted-foreground mt-1">
                              {result.reordered ? <span className="text-green-600">Reordered</span> : 'No change needed'}
                              {result.upcoming_count !== undefined && <span className="ml-2">{result.upcoming_count} upcoming, {result.past_count} past</span>}
                            </p>
                          ) : (
                            <p className="text-xs text-destructive mt-1">{result.error}</p>
                          )}
                        </Card>
                      ))}
                    </div>
                  )}
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  )
}
