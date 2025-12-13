import { useState, useEffect, useCallback, useRef } from 'react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card.jsx'
import { Button } from '@/components/ui/button.jsx'
import { Input } from '@/components/ui/input.jsx'
import { Label } from '@/components/ui/label.jsx'
import { Badge } from '@/components/ui/badge.jsx'
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle, DialogFooter } from '@/components/ui/dialog.jsx'
import { Accordion, AccordionContent, AccordionItem, AccordionTrigger } from '@/components/ui/accordion.jsx'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select.jsx'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs.jsx'
import { Alert, AlertDescription } from '@/components/ui/alert.jsx'
import { useToast } from '@/hooks/use-toast.js'
import { channelsAPI, regexAPI, streamCheckerAPI, channelSettingsAPI, channelOrderAPI } from '@/services/api.js'
import { CheckCircle, Edit, Plus, Trash2, Loader2, Search, X, Download, Upload, GripVertical, Save, RotateCcw, ArrowUpDown } from 'lucide-react'
import {
  DndContext,
  closestCenter,
  KeyboardSensor,
  PointerSensor,
  useSensor,
  useSensors,
} from '@dnd-kit/core'
import {
  arrayMove,
  SortableContext,
  sortableKeyboardCoordinates,
  useSortable,
  verticalListSortingStrategy,
} from '@dnd-kit/sortable'
import { CSS } from '@dnd-kit/utilities'

// Constants for localStorage keys
const CHANNEL_STATS_PREFIX = 'streamflow_channel_stats_'
const CHANNEL_LOGO_PREFIX = 'streamflow_channel_logo_'

function ChannelCard({ channel, patterns, onEditRegex, onDeletePattern, onCheckChannel, loading, channelSettings, onUpdateSettings }) {
  const [stats, setStats] = useState(null)
  const [loadingStats, setLoadingStats] = useState(true)
  const [expanded, setExpanded] = useState(false)
  const [logoUrl, setLogoUrl] = useState(null)
  const [logoError, setLogoError] = useState(false)
  const { toast } = useToast()

  const matchingMode = channelSettings?.matching_mode || 'enabled'
  const checkingMode = channelSettings?.checking_mode || 'enabled'

  const handleMatchingModeChange = async (value) => {
    try {
      await onUpdateSettings(channel.id, { matching_mode: value })
      toast({
        title: "Success",
        description: "Matching mode updated successfully"
      })
    } catch (err) {
      toast({
        title: "Error",
        description: "Failed to update matching mode",
        variant: "destructive"
      })
    }
  }

  const handleCheckingModeChange = async (value) => {
    try {
      await onUpdateSettings(channel.id, { checking_mode: value })
      toast({
        title: "Success",
        description: "Checking mode updated successfully"
      })
    } catch (err) {
      toast({
        title: "Error",
        description: "Failed to update checking mode",
        variant: "destructive"
      })
    }
  }

  useEffect(() => {
    // Try to load stats from localStorage first for instant display
    const cachedStats = localStorage.getItem(`${CHANNEL_STATS_PREFIX}${channel.id}`)
    if (cachedStats) {
      try {
        const parsed = JSON.parse(cachedStats)
        setStats(parsed)
        setLoadingStats(false) // Show cached data immediately
      } catch (e) {
        console.error('Failed to parse cached stats:', e)
      }
    }
    // Always fetch fresh stats in background to keep data current
    loadStats()
  }, [channel.id])

  // Fetch logo when logo_id is available
  useEffect(() => {
    const loadLogo = () => {
      // Try cached logo first from localStorage
      const cachedLogo = localStorage.getItem(`${CHANNEL_LOGO_PREFIX}${channel.id}`)
      if (cachedLogo) {
        setLogoUrl(cachedLogo)
      }
      
      // Set logo URL if logo_id is available using the cached endpoint
      // This endpoint will serve cached logos or download them on first request
      if (channel.logo_id) {
        const logoUrl = channelsAPI.getLogoCached(channel.logo_id)
        setLogoUrl(logoUrl)
        localStorage.setItem(`${CHANNEL_LOGO_PREFIX}${channel.id}`, logoUrl)
      }
    }
    loadLogo()
  }, [channel.id, channel.logo_id])

  const loadStats = async () => {
    try {
      setLoadingStats(true)
      const response = await channelsAPI.getChannelStats(channel.id)
      setStats(response.data)
      // Cache stats in localStorage
      localStorage.setItem(`${CHANNEL_STATS_PREFIX}${channel.id}`, JSON.stringify(response.data))
    } catch (err) {
      console.error('Failed to load channel stats:', err)
    } finally {
      setLoadingStats(false)
    }
  }

  const channelPatterns = patterns[channel.id] || patterns[String(channel.id)]

  return (
    <Card className="w-full">
      <CardContent className="p-0">
        <div className="flex items-center gap-3 p-3">
          {/* Channel Logo */}
          <div className="w-24 h-12 flex-shrink-0 bg-muted rounded-md flex items-center justify-center overflow-hidden">
            {logoUrl && !logoError ? (
              <img 
                src={logoUrl} 
                alt={channel.name} 
                className="w-full h-full object-contain"
                onError={() => setLogoError(true)}
              />
            ) : (
              <span className="text-2xl font-bold text-muted-foreground">
                {channel.name?.charAt(0) || '?'}
              </span>
            )}
          </div>

          {/* Channel Info */}
          <div className="flex-1 min-w-0">
            <h3 className="font-semibold text-base truncate">{channel.name}</h3>
            <div className="flex flex-wrap gap-3 mt-1 text-sm">
              {loadingStats ? (
                <span className="text-muted-foreground">Loading stats...</span>
              ) : stats ? (
                <>
                  <div className="flex items-center gap-1">
                    <span className="text-muted-foreground">Streams:</span>
                    <span className="font-medium">{stats.total_streams ?? 0}</span>
                  </div>
                  <div className="flex items-center gap-1">
                    <span className="text-muted-foreground">Dead:</span>
                    <span className="font-medium text-destructive">{stats.dead_streams ?? 0}</span>
                  </div>
                  <div className="flex items-center gap-1">
                    <span className="text-muted-foreground">Avg.Resolution:</span>
                    <span className="font-medium">{stats.most_common_resolution || 'Unknown'}</span>
                  </div>
                  <div className="flex items-center gap-1">
                    <span className="text-muted-foreground">Avg Bitrate:</span>
                    <span className="font-medium">{stats.average_bitrate > 0 ? `${stats.average_bitrate} Kbps` : 'N/A'}</span>
                  </div>
                </>
              ) : (
                <>
                  <div className="flex items-center gap-1">
                    <span className="text-muted-foreground">Streams:</span>
                    <span className="font-medium text-muted-foreground">--</span>
                  </div>
                  <div className="flex items-center gap-1">
                    <span className="text-muted-foreground">Dead:</span>
                    <span className="font-medium text-muted-foreground">--</span>
                  </div>
                  <div className="flex items-center gap-1">
                    <span className="text-muted-foreground">Avg.Resolution:</span>
                    <span className="font-medium text-muted-foreground">--</span>
                  </div>
                  <div className="flex items-center gap-1">
                    <span className="text-muted-foreground">Avg Bitrate:</span>
                    <span className="font-medium text-muted-foreground">--</span>
                  </div>
                </>
              )}
            </div>
          </div>

          {/* Action Buttons */}
          <div className="flex gap-2 flex-shrink-0">
            <Button
              variant="outline"
              size="sm"
              onClick={() => setExpanded(!expanded)}
            >
              <Edit className="h-4 w-4 mr-2" />
              Edit Regex
            </Button>
            <Button
              size="sm"
              onClick={() => onCheckChannel(channel.id)}
              disabled={loading}
            >
              {loading ? (
                <Loader2 className="h-4 w-4 mr-2 animate-spin" />
              ) : (
                <CheckCircle className="h-4 w-4 mr-2" />
              )}
              Check Channel
            </Button>
          </div>
        </div>

        {/* Expandable Regex Section */}
        {expanded && (
          <div className="border-t p-4 bg-muted/50 space-y-4">
            {/* Channel Settings */}
            <div className="grid grid-cols-2 gap-4 pb-4 border-b">
              <div className="space-y-2">
                <Label htmlFor={`matching-mode-${channel.id}`} className="text-sm font-medium">
                  Stream Matching
                </Label>
                <Select value={matchingMode} onValueChange={handleMatchingModeChange}>
                  <SelectTrigger id={`matching-mode-${channel.id}`}>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="enabled">Enabled</SelectItem>
                    <SelectItem value="disabled">Disabled</SelectItem>
                  </SelectContent>
                </Select>
                <p className="text-xs text-muted-foreground">
                  {matchingMode === 'enabled' 
                    ? 'Channel will be included in stream matching'
                    : 'Channel will be excluded from stream matching'}
                </p>
              </div>
              <div className="space-y-2">
                <Label htmlFor={`checking-mode-${channel.id}`} className="text-sm font-medium">
                  Stream Checking
                </Label>
                <Select value={checkingMode} onValueChange={handleCheckingModeChange}>
                  <SelectTrigger id={`checking-mode-${channel.id}`}>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="enabled">Enabled</SelectItem>
                    <SelectItem value="disabled">Disabled</SelectItem>
                  </SelectContent>
                </Select>
                <p className="text-xs text-muted-foreground">
                  {checkingMode === 'enabled'
                    ? 'Channel streams will be quality checked'
                    : 'Channel streams will not be quality checked'}
                </p>
              </div>
            </div>

            {/* Regex Patterns */}
            <div>
              <div className="flex justify-between items-center mb-3">
                <h4 className="font-medium text-sm">Regex Patterns</h4>
                <Button
                  size="sm"
                  variant="outline"
                  onClick={() => onEditRegex(channel.id, null)}
                >
                  <Plus className="h-4 w-4 mr-2" />
                  Add Pattern
                </Button>
              </div>
            
            {channelPatterns && channelPatterns.regex && channelPatterns.regex.length > 0 ? (
              <div className="space-y-2">
                {channelPatterns.regex.map((pattern, index) => (
                  <div key={index} className="flex items-center justify-between gap-2 p-2 bg-background rounded-md">
                    <code className="text-sm flex-1 break-all">{pattern}</code>
                    <div className="flex gap-1">
                      <Button
                        size="sm"
                        variant="ghost"
                        onClick={() => onEditRegex(channel.id, index)}
                      >
                        <Edit className="h-4 w-4" />
                      </Button>
                      <Button
                        size="sm"
                        variant="ghost"
                        onClick={() => onDeletePattern(channel.id, index)}
                      >
                        <Trash2 className="h-4 w-4" />
                      </Button>
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <p className="text-sm text-muted-foreground">No regex patterns configured</p>
            )}
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  )
}

function SortableChannelItem({ channel }) {
  const {
    attributes,
    listeners,
    setNodeRef,
    transform,
    transition,
    isDragging,
  } = useSortable({ id: channel.id })

  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
    opacity: isDragging ? 0.5 : 1,
  }

  return (
    <div
      ref={setNodeRef}
      style={style}
      className={`flex items-center gap-3 p-4 bg-card border rounded-lg ${
        isDragging ? 'shadow-lg' : 'shadow-sm'
      }`}
    >
      <div
        {...attributes}
        {...listeners}
        className="cursor-grab active:cursor-grabbing touch-none"
      >
        <GripVertical className="h-5 w-5 text-muted-foreground" />
      </div>
      
      <div className="flex-1 grid grid-cols-3 gap-4 items-center">
        <div>
          <Badge variant="outline" className="font-mono">
            #{channel.channel_number || 'N/A'}
          </Badge>
        </div>
        <div className="col-span-2">
          <div className="font-medium">{channel.name}</div>
          <div className="text-xs text-muted-foreground">ID: {channel.id}</div>
        </div>
      </div>
    </div>
  )
}

export default function ChannelConfiguration() {
  const [channels, setChannels] = useState([])
  const [patterns, setPatterns] = useState({})
  const [channelSettings, setChannelSettings] = useState({})
  const [loading, setLoading] = useState(true)
  const [checkingChannel, setCheckingChannel] = useState(null)
  const [searchQuery, setSearchQuery] = useState('')
  const [dialogOpen, setDialogOpen] = useState(false)
  const [editingChannelId, setEditingChannelId] = useState(null)
  const [editingPatternIndex, setEditingPatternIndex] = useState(null)
  const [newPattern, setNewPattern] = useState('')
  const [testingPattern, setTestingPattern] = useState(false)
  const [testResults, setTestResults] = useState(null)
  const testRequestIdRef = useRef(0)
  const { toast } = useToast()
  
  // Pagination state
  const [currentPage, setCurrentPage] = useState(1)
  const [itemsPerPage, setItemsPerPage] = useState(20)
  
  // Channel ordering state
  const [orderedChannels, setOrderedChannels] = useState([])
  const [originalChannelOrder, setOriginalChannelOrder] = useState([])
  const [hasOrderChanges, setHasOrderChanges] = useState(false)
  const [savingOrder, setSavingOrder] = useState(false)
  const [sortBy, setSortBy] = useState('custom')
  
  const sensors = useSensors(
    useSensor(PointerSensor),
    useSensor(KeyboardSensor, {
      coordinateGetter: sortableKeyboardCoordinates,
    })
  )

  useEffect(() => {
    loadData()
  }, [])

  const loadData = async () => {
    try {
      setLoading(true)
      const [channelsResponse, patternsResponse, settingsResponse] = await Promise.all([
        channelsAPI.getChannels(),
        regexAPI.getPatterns(),
        channelSettingsAPI.getAllSettings()
      ])
      
      setChannels(channelsResponse.data)
      setPatterns(patternsResponse.data.patterns || {})
      setChannelSettings(settingsResponse.data || {})
      
      // Initialize ordered channels
      const channelData = channelsResponse.data || []
      const sorted = [...channelData].sort((a, b) => {
        const numA = parseFloat(a.channel_number) || 999999
        const numB = parseFloat(b.channel_number) || 999999
        return numA - numB
      })
      setOrderedChannels(sorted)
      setOriginalChannelOrder(sorted)
      setHasOrderChanges(false)
    } catch (err) {
      console.error('Failed to load data:', err)
      toast({
        title: "Error",
        description: "Failed to load channel data",
        variant: "destructive"
      })
    } finally {
      setLoading(false)
    }
  }

  const handleUpdateSettings = async (channelId, settings) => {
    try {
      await channelSettingsAPI.updateSettings(channelId, settings)
      // Reload settings to get updated values
      const settingsResponse = await channelSettingsAPI.getAllSettings()
      setChannelSettings(settingsResponse.data || {})
    } catch (err) {
      throw err
    }
  }

  const handleCheckChannel = async (channelId) => {
    try {
      setCheckingChannel(channelId)
      
      // Show starting notification
      toast({
        title: "Channel Check Started",
        description: "Checking channel streams... This may take a few minutes.",
      })
      
      const response = await streamCheckerAPI.checkSingleChannel(channelId)
      
      if (response.data.success) {
        const stats = response.data.stats
        toast({
          title: "Channel Check Complete",
          description: `Checked ${stats.total_streams} streams. Dead: ${stats.dead_streams}. Avg Resolution: ${stats.avg_resolution}, Avg Bitrate: ${stats.avg_bitrate}`,
        })
        // Reload the channel data to show updated stats
        loadData()
      } else {
        toast({
          title: "Check Failed",
          description: response.data.error || "Failed to check channel",
          variant: "destructive"
        })
      }
    } catch (err) {
      console.error('Error checking channel:', err)
      // Check if it's a timeout error
      if (err.code === 'ECONNABORTED' || err.message?.includes('timeout')) {
        toast({
          title: "Check Taking Longer Than Expected",
          description: "The channel check is still running. Please check back in a few minutes.",
          variant: "default"
        })
      } else {
        toast({
          title: "Error",
          description: err.response?.data?.error || "Failed to check channel",
          variant: "destructive"
        })
      }
    } finally {
      setCheckingChannel(null)
    }
  }

  const handleEditRegex = (channelId, patternIndex) => {
    setEditingChannelId(channelId)
    setEditingPatternIndex(patternIndex)
    
    // If editing an existing pattern, load it
    if (patternIndex !== null) {
      const channelPatterns = patterns[channelId] || patterns[String(channelId)]
      if (channelPatterns && channelPatterns.regex && channelPatterns.regex[patternIndex]) {
        setNewPattern(channelPatterns.regex[patternIndex])
      }
    } else {
      setNewPattern('')
    }
    
    setTestResults(null)
    setDialogOpen(true)
  }

  const handleCloseDialog = () => {
    setDialogOpen(false)
    setEditingChannelId(null)
    setEditingPatternIndex(null)
    setNewPattern('')
    setTestResults(null)
  }

  const handleTestPattern = useCallback(async () => {
    if (!newPattern.trim() || !editingChannelId) return
    
    // Increment request ID to track this request
    const requestId = ++testRequestIdRef.current
    
    try {
      setTestingPattern(true)
      const channel = channels.find(ch => ch.id === editingChannelId)
      const response = await regexAPI.testPatternLive({
        patterns: [{
          channel_id: editingChannelId,
          channel_name: channel?.name || '',
          regex: [newPattern]
        }],
        max_matches: 50
      })
      
      // Only update state if this is still the latest request
      if (requestId !== testRequestIdRef.current) return
      
      // Extract results for this channel
      const result = response.data.results?.[0]
      if (result) {
        setTestResults({
          valid: true,
          matches: result.matched_streams?.map(s => s.stream_name) || [],
          match_count: result.match_count || 0
        })
      } else {
        setTestResults({ valid: true, matches: [], match_count: 0 })
      }
    } catch (err) {
      // Only update state if this is still the latest request
      if (requestId !== testRequestIdRef.current) return
      
      // Check if it's a validation error
      if (err.response?.data?.error) {
        setTestResults({
          valid: false,
          error: err.response.data.error
        })
      } else {
        toast({
          title: "Error",
          description: "Failed to test pattern",
          variant: "destructive"
        })
      }
    } finally {
      // Only update loading state if this is still the latest request
      if (requestId === testRequestIdRef.current) {
        setTestingPattern(false)
      }
    }
  }, [newPattern, editingChannelId, channels, toast])

  // Test pattern on every change with debouncing
  useEffect(() => {
    if (!newPattern || !editingChannelId || !dialogOpen) {
      setTestResults(null)
      return
    }

    const timer = setTimeout(() => {
      handleTestPattern()
    }, 500) // 500ms debounce
    
    return () => clearTimeout(timer)
    // Only depend on the actual values, not the function
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [newPattern, editingChannelId, dialogOpen])

  const handleSavePattern = async () => {
    if (!newPattern.trim() || !editingChannelId) {
      toast({
        title: "Error",
        description: "Pattern cannot be empty",
        variant: "destructive"
      })
      return
    }

    try {
      const channelPatterns = patterns[editingChannelId] || patterns[String(editingChannelId)]
      const channel = channels.find(ch => ch.id === editingChannelId)
      
      let updatedRegex = []
      if (editingPatternIndex !== null && channelPatterns?.regex) {
        // Editing existing pattern
        updatedRegex = [...channelPatterns.regex]
        updatedRegex[editingPatternIndex] = newPattern
      } else {
        // Adding new pattern
        updatedRegex = channelPatterns?.regex ? [...channelPatterns.regex, newPattern] : [newPattern]
      }

      await regexAPI.addPattern({
        channel_id: editingChannelId,
        name: channel?.name || '',
        regex: updatedRegex,
        enabled: channelPatterns?.enabled !== false
      })

      toast({
        title: "Success",
        description: editingPatternIndex !== null ? "Pattern updated successfully" : "Pattern added successfully"
      })

      // Reload patterns
      await loadData()
      handleCloseDialog()
    } catch (err) {
      toast({
        title: "Error",
        description: "Failed to save pattern",
        variant: "destructive"
      })
    }
  }

  const handleDeletePattern = async (channelId, patternIndex) => {
    try {
      const channelPatterns = patterns[channelId] || patterns[String(channelId)]
      const channel = channels.find(ch => ch.id === channelId)
      
      if (!channelPatterns?.regex) return

      const updatedRegex = channelPatterns.regex.filter((_, index) => index !== patternIndex)
      
      if (updatedRegex.length === 0) {
        // If no patterns left, delete the entire pattern config
        await regexAPI.deletePattern(channelId)
      } else {
        // Update with remaining patterns
        await regexAPI.addPattern({
          channel_id: channelId,
          name: channel?.name || '',
          regex: updatedRegex,
          enabled: channelPatterns.enabled !== false
        })
      }

      toast({
        title: "Success",
        description: "Pattern deleted successfully"
      })

      await loadData()
    } catch (err) {
      toast({
        title: "Error",
        description: "Failed to delete pattern",
        variant: "destructive"
      })
    }
  }

  // Filter channels based on search query
  const filteredChannels = channels.filter(channel => {
    if (!searchQuery.trim()) return true
    
    const query = searchQuery.toLowerCase()
    const channelName = (channel.name || '').toLowerCase()
    const channelNumber = channel.channel_number ? String(channel.channel_number) : ''
    const channelId = String(channel.id)
    
    return channelName.includes(query) || 
           channelNumber.includes(query) || 
           channelId.includes(query)
  })

  // Calculate pagination
  const totalPages = Math.ceil(filteredChannels.length / itemsPerPage)
  const startIndex = (currentPage - 1) * itemsPerPage
  const endIndex = startIndex + itemsPerPage
  const paginatedChannels = filteredChannels.slice(startIndex, endIndex)

  // Reset to first page when search changes
  useEffect(() => {
    setCurrentPage(1)
  }, [searchQuery])

  const clearSearch = () => {
    setSearchQuery('')
  }
  
  // Channel ordering handlers
  const handleDragEnd = (event) => {
    const { active, over } = event

    if (over && active.id !== over.id) {
      setOrderedChannels((items) => {
        const oldIndex = items.findIndex((item) => item.id === active.id)
        const newIndex = items.findIndex((item) => item.id === over.id)
        const newOrder = arrayMove(items, oldIndex, newIndex)
        setHasOrderChanges(true)
        return newOrder
      })
    }
  }

  const handleSort = (value) => {
    setSortBy(value)
    
    let sorted = [...orderedChannels]
    
    switch (value) {
      case 'channel_number':
        sorted.sort((a, b) => {
          const numA = parseFloat(a.channel_number) || 999999
          const numB = parseFloat(b.channel_number) || 999999
          return numA - numB
        })
        break
      case 'name':
        sorted.sort((a, b) => a.name.localeCompare(b.name))
        break
      case 'id':
        sorted.sort((a, b) => a.id - b.id)
        break
      case 'custom':
        // Keep current order
        return
    }
    
    setOrderedChannels(sorted)
    setHasOrderChanges(JSON.stringify(sorted) !== JSON.stringify(originalChannelOrder))
  }

  const handleSaveOrder = async () => {
    try {
      setSavingOrder(true)
      
      // Create order array with channel IDs
      const order = orderedChannels.map(ch => ch.id)
      
      await channelOrderAPI.setOrder(order)
      
      setOriginalChannelOrder([...orderedChannels])
      setHasOrderChanges(false)
      
      toast({
        title: "Success",
        description: "Channel order saved successfully"
      })
    } catch (err) {
      console.error('Failed to save order:', err)
      toast({
        title: "Error",
        description: "Failed to save channel order",
        variant: "destructive"
      })
    } finally {
      setSavingOrder(false)
    }
  }

  const handleResetOrder = () => {
    setOrderedChannels([...originalChannelOrder])
    setHasOrderChanges(false)
    setSortBy('custom')
    toast({
      title: "Reset",
      description: "Changes have been discarded"
    })
  }

  const handleExportPatterns = () => {
    try {
      // Create a JSON blob with the current patterns
      const dataStr = JSON.stringify({ patterns }, null, 2)
      const dataBlob = new Blob([dataStr], { type: 'application/json' })
      
      // Create download link
      const url = URL.createObjectURL(dataBlob)
      const link = document.createElement('a')
      link.href = url
      link.download = `channel_regex_patterns_${new Date().toISOString().split('T')[0]}.json`
      document.body.appendChild(link)
      link.click()
      document.body.removeChild(link)
      URL.revokeObjectURL(url)
      
      toast({
        title: "Success",
        description: "Regex patterns exported successfully"
      })
    } catch (err) {
      console.error('Export error:', err)
      toast({
        title: "Error",
        description: "Failed to export patterns",
        variant: "destructive"
      })
    }
  }

  const handleImportPatterns = (event) => {
    const file = event.target.files[0]
    if (!file) return
    
    const reader = new FileReader()
    reader.onload = async (e) => {
      try {
        const importedData = JSON.parse(e.target.result)
        
        // Validate the imported data structure
        if (!importedData.patterns || typeof importedData.patterns !== 'object') {
          throw new Error('Invalid file format: missing patterns object')
        }
        
        // Import the patterns using the API
        await regexAPI.importPatterns(importedData)
        
        toast({
          title: "Success",
          description: `Imported ${Object.keys(importedData.patterns).length} channel patterns`
        })
        
        // Reload data to show imported patterns
        await loadData()
      } catch (err) {
        console.error('Import error:', err)
        toast({
          title: "Error",
          description: err.response?.data?.error || "Failed to import patterns",
          variant: "destructive"
        })
      }
    }
    
    reader.onerror = () => {
      toast({
        title: "Error",
        description: "Failed to read file",
        variant: "destructive"
      })
    }
    
    reader.readAsText(file)
    
    // Reset the input so the same file can be imported again
    event.target.value = ''
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-[400px]">
        <Loader2 className="h-8 w-8 animate-spin text-primary" />
      </div>
    )
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold tracking-tight">Channel Configuration</h1>
        <p className="text-muted-foreground">
          View and manage channel regex patterns, settings, and ordering
        </p>
      </div>

      <Tabs defaultValue="regex" className="w-full">
        <TabsList className="grid w-full grid-cols-2">
          <TabsTrigger value="regex">Regex Configuration</TabsTrigger>
          <TabsTrigger value="ordering">Channel Order</TabsTrigger>
        </TabsList>
        
        <TabsContent value="regex" className="space-y-6">
          {/* Search Bar and Export/Import Buttons */}
          <div className="flex items-center gap-2">
            <div className="relative flex-1 max-w-md">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-muted-foreground" />
              <Input
                type="text"
                placeholder="Search channels by name, number, or ID..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="pl-10 pr-10"
              />
              {searchQuery && (
                <Button
                  variant="ghost"
                  size="sm"
                  className="absolute right-1 top-1/2 transform -translate-y-1/2 h-7 w-7 p-0"
                  onClick={clearSearch}
                >
                  <X className="h-4 w-4" />
                </Button>
              )}
            </div>
            {searchQuery && (
              <Badge variant="secondary">
                {filteredChannels.length} of {channels.length} channels
              </Badge>
            )}
            
            {/* Export/Import Buttons */}
            <div className="flex items-center gap-2 ml-auto">
              <Button
                variant="outline"
                size="sm"
                onClick={handleExportPatterns}
              >
                <Download className="h-4 w-4 mr-2" />
                Export Regex
              </Button>
              <Button
                variant="outline"
                size="sm"
                onClick={() => document.getElementById('import-file-input').click()}
              >
                <Upload className="h-4 w-4 mr-2" />
                Import Regex
              </Button>
              <input
                id="import-file-input"
                type="file"
                accept=".json"
                onChange={handleImportPatterns}
                style={{ display: 'none' }}
              />
            </div>
          </div>

          <div className="space-y-4">
            {/* Pagination info and controls at top */}
            {filteredChannels.length > 0 && (
              <div className="flex items-center justify-between">
                <div className="text-sm text-muted-foreground">
                  Showing {startIndex + 1}-{Math.min(endIndex, filteredChannels.length)} of {filteredChannels.length} channels
                </div>
                <div className="flex items-center gap-2">
                  <Label htmlFor="items-per-page" className="text-sm whitespace-nowrap">Items per page:</Label>
                  <Select
                    value={itemsPerPage.toString()}
                    onValueChange={(value) => {
                      setItemsPerPage(Number(value))
                      setCurrentPage(1)
                    }}
                  >
                    <SelectTrigger className="h-9 w-[100px]">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="10">10</SelectItem>
                      <SelectItem value="20">20</SelectItem>
                      <SelectItem value="50">50</SelectItem>
                      <SelectItem value="100">100</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>
            )}

            {filteredChannels.length === 0 ? (
              <Card>
                <CardContent className="p-8 text-center">
                  <p className="text-muted-foreground">
                    {searchQuery ? `No channels found matching "${searchQuery}"` : 'No channels available'}
                  </p>
                  {searchQuery && (
                    <Button
                      variant="outline"
                      size="sm"
                      className="mt-4"
                      onClick={clearSearch}
                    >
                      Clear search
                    </Button>
                  )}
                </CardContent>
              </Card>
            ) : (
              paginatedChannels.map(channel => (
                <ChannelCard
                  key={channel.id}
                  channel={channel}
                  patterns={patterns}
                  channelSettings={channelSettings[channel.id]}
                  onEditRegex={handleEditRegex}
                  onDeletePattern={handleDeletePattern}
                  onCheckChannel={handleCheckChannel}
                  onUpdateSettings={handleUpdateSettings}
                  loading={checkingChannel === channel.id}
                />
              ))
            )}

            {/* Pagination controls at bottom */}
            {totalPages > 1 && (
              <div className="flex items-center justify-center gap-2 pt-4">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setCurrentPage(1)}
                  disabled={currentPage === 1}
                >
                  First
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setCurrentPage(currentPage - 1)}
                  disabled={currentPage === 1}
                >
                  Previous
                </Button>
                <div className="flex items-center gap-1">
                  {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
                    // Show pages around current page
                    let pageNum
                    if (totalPages <= 5) {
                      pageNum = i + 1
                    } else if (currentPage <= 3) {
                      pageNum = i + 1
                    } else if (currentPage >= totalPages - 2) {
                      pageNum = totalPages - 4 + i
                    } else {
                      pageNum = currentPage - 2 + i
                    }
                    
                    return (
                      <Button
                        key={pageNum}
                        variant={currentPage === pageNum ? "default" : "outline"}
                        size="sm"
                        onClick={() => setCurrentPage(pageNum)}
                        className="w-9"
                      >
                        {pageNum}
                      </Button>
                    )
                  })}
                </div>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setCurrentPage(currentPage + 1)}
                  disabled={currentPage === totalPages}
                >
                  Next
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setCurrentPage(totalPages)}
                  disabled={currentPage === totalPages}
                >
                  Last
                </Button>
              </div>
            )}
          </div>
        </TabsContent>
        
        <TabsContent value="ordering" className="space-y-6">
          {hasOrderChanges && (
            <Alert>
              <AlertDescription className="flex items-center justify-between">
                <span>You have unsaved changes</span>
                <div className="flex gap-2">
                  <Button size="sm" variant="outline" onClick={handleResetOrder}>
                    <RotateCcw className="h-4 w-4 mr-2" />
                    Reset
                  </Button>
                  <Button size="sm" onClick={handleSaveOrder} disabled={savingOrder}>
                    {savingOrder && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                    <Save className="h-4 w-4 mr-2" />
                    Save Changes
                  </Button>
                </div>
              </AlertDescription>
            </Alert>
          )}

          <Card>
            <CardHeader>
              <div className="flex items-center justify-between">
                <div>
                  <CardTitle>Channel List</CardTitle>
                  <CardDescription>
                    {orderedChannels.length} channels total - Drag and drop to reorder
                  </CardDescription>
                </div>
                <div className="flex items-center gap-2">
                  <ArrowUpDown className="h-4 w-4 text-muted-foreground" />
                  <Select value={sortBy} onValueChange={handleSort}>
                    <SelectTrigger className="w-[200px]">
                      <SelectValue placeholder="Sort by..." />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="custom">Custom Order</SelectItem>
                      <SelectItem value="channel_number">Channel Number</SelectItem>
                      <SelectItem value="name">Name (A-Z)</SelectItem>
                      <SelectItem value="id">ID</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>
            </CardHeader>
            <CardContent>
              {orderedChannels.length === 0 ? (
                <div className="text-center py-8 text-muted-foreground">
                  No channels available
                </div>
              ) : (
                <DndContext
                  sensors={sensors}
                  collisionDetection={closestCenter}
                  onDragEnd={handleDragEnd}
                >
                  <SortableContext
                    items={orderedChannels.map(ch => ch.id)}
                    strategy={verticalListSortingStrategy}
                  >
                    <div className="space-y-2">
                      {orderedChannels.map((channel) => (
                        <SortableChannelItem key={channel.id} channel={channel} />
                      ))}
                    </div>
                  </SortableContext>
                </DndContext>
              )}
            </CardContent>
          </Card>

          {hasOrderChanges && (
            <div className="flex justify-end gap-2">
              <Button variant="outline" onClick={handleResetOrder}>
                <RotateCcw className="h-4 w-4 mr-2" />
                Reset Changes
              </Button>
              <Button onClick={handleSaveOrder} disabled={savingOrder} size="lg">
                {savingOrder && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                <Save className="h-4 w-4 mr-2" />
                Save Order
              </Button>
            </div>
          )}
        </TabsContent>
      </Tabs>

      {/* Regex Pattern Dialog */}
      <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
        <DialogContent className="sm:max-w-[600px]">
          <DialogHeader>
            <DialogTitle>
              {editingPatternIndex !== null ? 'Edit' : 'Add'} Regex Pattern
            </DialogTitle>
            <DialogDescription>
              Enter a regex pattern to match streams for this channel.
            </DialogDescription>
          </DialogHeader>
          
          <div className="space-y-4 py-4">
            <div className="space-y-2">
              <Label htmlFor="pattern">Regex Pattern</Label>
              <Input
                id="pattern"
                placeholder="e.g., .*ESPN.*|.*Sports.*"
                value={newPattern}
                onChange={(e) => setNewPattern(e.target.value)}
                className="font-mono"
              />
            </div>

            {/* Live Test Results */}
            {testingPattern && (
              <div className="flex items-center gap-2 text-sm text-muted-foreground animate-in fade-in duration-200">
                <Loader2 className="h-4 w-4 animate-spin" />
                Testing pattern...
              </div>
            )}

            {testResults && !testingPattern && (
              <div className="space-y-2 animate-in fade-in slide-in-from-top-2 duration-300">
                <Label>Test Results</Label>
                <div className="border rounded-md p-3 bg-muted/50 transition-all">
                  {testResults.valid ? (
                    <>
                      <div className="flex items-center gap-2 text-sm font-medium text-green-600 mb-2 animate-in fade-in duration-200">
                        <CheckCircle className="h-4 w-4" />
                        Valid pattern - {testResults.matches?.length || 0} matches found
                      </div>
                      {testResults.matches && testResults.matches.length > 0 && (
                        <div className="space-y-1 max-h-32 overflow-y-auto">
                          {testResults.matches.slice(0, 10).map((match, idx) => (
                            <div 
                              key={idx} 
                              className="text-xs text-muted-foreground truncate animate-in fade-in slide-in-from-left-1 duration-200"
                              style={{ animationDelay: `${idx * 20}ms` }}
                            >
                              • {match}
                            </div>
                          ))}
                          {testResults.matches.length > 10 && (
                            <div className="text-xs text-muted-foreground italic animate-in fade-in duration-200">
                              ... and {testResults.matches.length - 10} more
                            </div>
                          )}
                        </div>
                      )}
                    </>
                  ) : (
                    <div className="text-sm text-destructive">
                      {testResults.error || 'Invalid pattern'}
                    </div>
                  )}
                </div>
              </div>
            )}
          </div>

          <DialogFooter>
            <Button variant="outline" onClick={handleCloseDialog}>
              Cancel
            </Button>
            <Button onClick={handleSavePattern} disabled={!newPattern.trim()}>
              {editingPatternIndex !== null ? 'Update' : 'Add'} Pattern
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}
