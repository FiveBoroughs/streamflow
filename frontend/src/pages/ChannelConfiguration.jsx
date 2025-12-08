import { useState, useEffect } from 'react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card.jsx'
import { Button } from '@/components/ui/button.jsx'
import { Input } from '@/components/ui/input.jsx'
import { Label } from '@/components/ui/label.jsx'
import { Badge } from '@/components/ui/badge.jsx'
import { Accordion, AccordionContent, AccordionItem, AccordionTrigger } from '@/components/ui/accordion.jsx'
import { useToast } from '@/hooks/use-toast.js'
import { channelsAPI, regexAPI, streamCheckerAPI } from '@/services/api.js'
import { CheckCircle, Edit, Plus, Trash2, Loader2 } from 'lucide-react'

function ChannelCard({ channel, patterns, onEditRegex, onCheckChannel, loading }) {
  const [stats, setStats] = useState(null)
  const [loadingStats, setLoadingStats] = useState(true)
  const [expanded, setExpanded] = useState(false)

  useEffect(() => {
    loadStats()
  }, [channel.id])

  const loadStats = async () => {
    try {
      setLoadingStats(true)
      const response = await channelsAPI.getChannelStats(channel.id)
      setStats(response.data)
    } catch (err) {
      console.error('Failed to load channel stats:', err)
    } finally {
      setLoadingStats(false)
    }
  }

  const channelPatterns = patterns[channel.id]

  return (
    <Card className="w-full">
      <CardContent className="p-0">
        <div className="flex items-center gap-4 p-4">
          {/* Channel Logo */}
          <div className="w-16 h-16 flex-shrink-0 bg-muted rounded-md flex items-center justify-center overflow-hidden">
            {channel.logo_url ? (
              <img src={channel.logo_url} alt={channel.name} className="w-full h-full object-cover" />
            ) : (
              <span className="text-2xl font-bold text-muted-foreground">
                {channel.name?.charAt(0) || '?'}
              </span>
            )}
          </div>

          {/* Channel Info */}
          <div className="flex-1 min-w-0">
            <h3 className="font-semibold text-lg truncate">{channel.name}</h3>
            <div className="flex flex-wrap gap-4 mt-2 text-sm">
              {loadingStats ? (
                <span className="text-muted-foreground">Loading stats...</span>
              ) : stats ? (
                <>
                  <div className="flex items-center gap-1">
                    <span className="text-muted-foreground">Streams:</span>
                    <span className="font-medium">{stats.total_streams}</span>
                  </div>
                  <div className="flex items-center gap-1">
                    <span className="text-muted-foreground">Dead:</span>
                    <span className="font-medium text-destructive">{stats.dead_streams}</span>
                  </div>
                  <div className="flex items-center gap-1">
                    <span className="text-muted-foreground">Resolution:</span>
                    <span className="font-medium">{stats.most_common_resolution}</span>
                  </div>
                  <div className="flex items-center gap-1">
                    <span className="text-muted-foreground">Avg Bitrate:</span>
                    <span className="font-medium">{stats.average_bitrate > 0 ? `${(stats.average_bitrate / 1000).toFixed(0)} Kbps` : 'N/A'}</span>
                  </div>
                </>
              ) : (
                <span className="text-muted-foreground">No stats available</span>
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
                  <div key={index} className="flex items-center justify-between p-2 bg-background rounded-md">
                    <code className="text-sm flex-1">{pattern}</code>
                    <Button
                      size="sm"
                      variant="ghost"
                      onClick={() => onEditRegex(channel.id, index)}
                    >
                      <Trash2 className="h-4 w-4" />
                    </Button>
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
    // This would open a dialog to edit/add regex patterns
    // For now, we'll show a simple toast
    toast({
      title: "Edit Regex",
      description: "Regex editing dialog will open here"
    })
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

      <div className="space-y-4">
        {channels.length === 0 ? (
          <Card>
            <CardContent className="p-8 text-center">
              <p className="text-muted-foreground">No channels available</p>
            </CardContent>
          </Card>
        ) : (
          channels.map(channel => (
            <ChannelCard
              key={channel.id}
              channel={channel}
              patterns={patterns}
              onEditRegex={handleEditRegex}
              onCheckChannel={handleCheckChannel}
              loading={checkingChannel === channel.id}
            />
          ))
        )}
      </div>
    </div>
  )
}
