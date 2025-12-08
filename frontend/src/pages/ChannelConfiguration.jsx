import { useState, useEffect, useCallback, useRef } from 'react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card.jsx'
import { Button } from '@/components/ui/button.jsx'
import { Input } from '@/components/ui/input.jsx'
import { Label } from '@/components/ui/label.jsx'
import { Badge } from '@/components/ui/badge.jsx'
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle, DialogFooter } from '@/components/ui/dialog.jsx'
import { Accordion, AccordionContent, AccordionItem, AccordionTrigger } from '@/components/ui/accordion.jsx'
import { useToast } from '@/hooks/use-toast.js'
import { channelsAPI, regexAPI, streamCheckerAPI } from '@/services/api.js'
import { CheckCircle, Edit, Plus, Trash2, Loader2, Search, X } from 'lucide-react'

// Constants for localStorage keys
const CHANNEL_STATS_PREFIX = 'streamflow_channel_stats_'
const CHANNEL_LOGO_PREFIX = 'streamflow_channel_logo_'

function ChannelCard({ channel, patterns, onEditRegex, onDeletePattern, onCheckChannel, loading }) {
  const [stats, setStats] = useState(null)
  const [loadingStats, setLoadingStats] = useState(true)
  const [expanded, setExpanded] = useState(false)
  const [logoUrl, setLogoUrl] = useState(null)

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
    const loadLogo = async () => {
      // Try cached logo first
      const cachedLogo = localStorage.getItem(`${CHANNEL_LOGO_PREFIX}${channel.id}`)
      if (cachedLogo) {
        setLogoUrl(cachedLogo)
      }
      
      // Fetch fresh logo if logo_id is available
      if (channel.logo_id) {
        try {
          const response = await channelsAPI.getLogo(channel.logo_id)
          if (response.data && (response.data.cache_url || response.data.url)) {
            const url = response.data.cache_url || response.data.url
            setLogoUrl(url)
            localStorage.setItem(`${CHANNEL_LOGO_PREFIX}${channel.id}`, url)
          }
        } catch (err) {
          console.error('Failed to load logo:', err)
        }
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
    <Card className="w-full max-w-4xl">
      <CardContent className="p-0">
        <div className="flex items-center gap-3 p-3">
          {/* Channel Logo */}
          <div className="w-12 h-12 flex-shrink-0 bg-muted rounded-md flex items-center justify-center overflow-hidden">
            {logoUrl ? (
              <img src={logoUrl} alt={channel.name} className="w-full h-full object-cover" />
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
                    <span className="text-muted-foreground">Resolution:</span>
                    <span className="font-medium">{stats.most_common_resolution || 'Unknown'}</span>
                  </div>
                  <div className="flex items-center gap-1">
                    <span className="text-muted-foreground">Avg Bitrate:</span>
                    <span className="font-medium">{stats.average_bitrate > 0 ? `${(stats.average_bitrate / 1000).toFixed(0)} Kbps` : 'N/A'}</span>
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
                    <span className="text-muted-foreground">Resolution:</span>
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
          <div className="border-t p-4 bg-muted/50">
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
        )}
      </CardContent>
    </Card>
  )
}

export default function ChannelConfiguration() {
  const [channels, setChannels] = useState([])
  const [patterns, setPatterns] = useState({})
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

  useEffect(() => {
    loadData()
  }, [])

  const loadData = async () => {
    try {
      setLoading(true)
      const [channelsResponse, patternsResponse] = await Promise.all([
        channelsAPI.getChannels(),
        regexAPI.getPatterns()
      ])
      
      setChannels(channelsResponse.data)
      setPatterns(patternsResponse.data.patterns || {})
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

  const handleCheckChannel = async (channelId) => {
    try {
      setCheckingChannel(channelId)
      await streamCheckerAPI.checkChannel(channelId)
      toast({
        title: "Success",
        description: "Channel queued for checking"
      })
    } catch (err) {
      toast({
        title: "Error",
        description: "Failed to queue channel for checking",
        variant: "destructive"
      })
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

  const clearSearch = () => {
    setSearchQuery('')
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
          View and manage channel regex patterns and stream assignments
        </p>
      </div>

      {/* Search Bar */}
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
      </div>

      <div className="space-y-4">
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
          filteredChannels.map(channel => (
            <ChannelCard
              key={channel.id}
              channel={channel}
              patterns={patterns}
              onEditRegex={handleEditRegex}
              onDeletePattern={handleDeletePattern}
              onCheckChannel={handleCheckChannel}
              loading={checkingChannel === channel.id}
            />
          ))
        )}
      </div>

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
              <div className="flex items-center gap-2 text-sm text-muted-foreground">
                <Loader2 className="h-4 w-4 animate-spin" />
                Testing pattern...
              </div>
            )}

            {testResults && !testingPattern && (
              <div className="space-y-2">
                <Label>Test Results</Label>
                <div className="border rounded-md p-3 bg-muted/50">
                  {testResults.valid ? (
                    <>
                      <div className="flex items-center gap-2 text-sm font-medium text-green-600 mb-2">
                        <CheckCircle className="h-4 w-4" />
                        Valid pattern - {testResults.matches?.length || 0} matches found
                      </div>
                      {testResults.matches && testResults.matches.length > 0 && (
                        <div className="space-y-1 max-h-32 overflow-y-auto">
                          {testResults.matches.slice(0, 10).map((match, idx) => (
                            <div key={idx} className="text-xs text-muted-foreground truncate">
                              • {match}
                            </div>
                          ))}
                          {testResults.matches.length > 10 && (
                            <div className="text-xs text-muted-foreground italic">
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
