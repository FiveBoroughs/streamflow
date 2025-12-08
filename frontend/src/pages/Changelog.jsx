import { useState, useEffect } from 'react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card.jsx'
import { Badge } from '@/components/ui/badge.jsx'
import { Accordion, AccordionContent, AccordionItem, AccordionTrigger } from '@/components/ui/accordion.jsx'
import { useToast } from '@/hooks/use-toast.js'
import { changelogAPI } from '@/services/api.js'
import { Loader2, CheckCircle2, AlertCircle, Activity } from 'lucide-react'

function formatTimestamp(timestamp) {
  const date = new Date(timestamp)
  const now = new Date()
  const diffMs = now - date
  const diffMins = Math.floor(diffMs / 60000)
  const diffHours = Math.floor(diffMs / 3600000)
  const diffDays = Math.floor(diffMs / 86400000)
  
  if (diffMins < 1) return 'Just now'
  if (diffMins < 60) return `${diffMins}m ago`
  if (diffHours < 24) return `${diffHours}h ago`
  if (diffDays < 7) return `${diffDays}d ago`
  
  return date.toLocaleDateString() + ' ' + date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
}

function getActionLabel(action) {
  switch (action) {
    case 'playlist_update_match':
      return 'Playlist Update & Match'
    case 'global_check':
      return 'Global Check'
    case 'single_channel_check':
      return 'Single Channel Check'
    case 'playlist_refresh':
      return 'Playlist Refresh'
    case 'streams_assigned':
      return 'Streams Assigned'
    case 'stream_check':
      return 'Stream Check'
    default:
      return action
  }
}

function getActionIcon(action) {
  switch (action) {
    case 'playlist_update_match':
    case 'global_check':
    case 'single_channel_check':
      return <CheckCircle2 className="h-4 w-4" />
    case 'playlist_refresh':
      return <Activity className="h-4 w-4" />
    default:
      return <AlertCircle className="h-4 w-4" />
  }
}

function getActionColor(action) {
  switch (action) {
    case 'playlist_update_match':
      return 'bg-blue-500/10 text-blue-500 border-blue-500/20'
    case 'global_check':
      return 'bg-green-500/10 text-green-500 border-green-500/20'
    case 'single_channel_check':
      return 'bg-purple-500/10 text-purple-500 border-purple-500/20'
    case 'playlist_refresh':
      return 'bg-orange-500/10 text-orange-500 border-orange-500/20'
    default:
      return 'bg-gray-500/10 text-gray-500 border-gray-500/20'
  }
}

function ChangelogEntry({ entry }) {
  const { timestamp, action, details, subentries } = entry
  const hasSubentries = subentries && subentries.length > 0
  
  return (
    <Card className="overflow-hidden">
      <CardHeader className="pb-3">
        <div className="flex items-start justify-between">
          <div className="flex items-center gap-2">
            <Badge variant="outline" className={`${getActionColor(action)} border`}>
              {getActionIcon(action)}
              <span className="ml-1">{getActionLabel(action)}</span>
            </Badge>
          </div>
          <span className="text-sm text-muted-foreground">{formatTimestamp(timestamp)}</span>
        </div>
        
        {/* Global Stats */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mt-3 pt-3 border-t">
          {details.total_streams !== undefined && (
            <div>
              <p className="text-xs text-muted-foreground">Total Streams</p>
              <p className="text-lg font-semibold">{details.total_streams}</p>
            </div>
          )}
          {details.dead_streams !== undefined && (
            <div>
              <p className="text-xs text-muted-foreground">Dead Streams</p>
              <p className="text-lg font-semibold text-destructive">{details.dead_streams}</p>
            </div>
          )}
          {details.avg_resolution && (
            <div>
              <p className="text-xs text-muted-foreground">Avg Resolution</p>
              <p className="text-lg font-semibold">{details.avg_resolution}</p>
            </div>
          )}
          {details.avg_bitrate && (
            <div>
              <p className="text-xs text-muted-foreground">Avg Bitrate</p>
              <p className="text-lg font-semibold">{details.avg_bitrate}</p>
            </div>
          )}
          {details.channel_name && (
            <div className="col-span-2">
              <p className="text-xs text-muted-foreground">Channel</p>
              <p className="text-lg font-semibold truncate">{details.channel_name}</p>
            </div>
          )}
        </div>
      </CardHeader>
      
      {/* Subentries */}
      {hasSubentries && (
        <CardContent className="pt-0">
          <Accordion type="multiple" className="w-full">
            {subentries.map((group, groupIndex) => {
              const groupType = group.group
              const items = group.items || []
              
              return (
                <AccordionItem key={groupIndex} value={`group-${groupIndex}`}>
                  <AccordionTrigger className="hover:no-underline">
                    <div className="flex items-center gap-2">
                      <span className="font-medium">
                        {groupType === 'update_match' ? 'Update & Match' : 'Channel Checks'}
                      </span>
                      <Badge variant="secondary">{items.length}</Badge>
                    </div>
                  </AccordionTrigger>
                  <AccordionContent>
                    <div className="space-y-3 pt-2">
                      {items.map((item, itemIndex) => (
                        <Card key={itemIndex} className="border-l-4 border-l-primary/50">
                          <CardContent className="p-3">
                            <div className="space-y-2">
                              <div className="flex items-center justify-between">
                                <h4 className="font-medium">{item.channel_name}</h4>
                                <Badge variant="outline" className="text-xs">
                                  Channel #{item.channel_id}
                                </Badge>
                              </div>
                              
                              {/* Stream list for update_match */}
                              {groupType === 'update_match' && item.streams && (
                                <div className="mt-2">
                                  <p className="text-xs text-muted-foreground mb-1">Streams Added:</p>
                                  <ul className="list-disc list-inside text-sm space-y-1 ml-2">
                                    {item.streams.slice(0, 5).map((stream, idx) => (
                                      <li key={stream.id || stream.stream_id || `stream-${itemIndex}-${idx}`} className="truncate">
                                        {stream.name || stream.stream_name || `Stream ${stream.id || stream.stream_id}`}
                                      </li>
                                    ))}
                                    {item.streams.length > 5 && (
                                      <li key="more" className="text-muted-foreground">
                                        +{item.streams.length - 5} more...
                                      </li>
                                    )}
                                  </ul>
                                </div>
                              )}
                              
                              {/* Stats for check */}
                              {groupType === 'check' && item.stats && (
                                <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mt-2">
                                  {item.stats.total_streams !== undefined && (
                                    <div>
                                      <p className="text-xs text-muted-foreground">Streams</p>
                                      <p className="text-sm font-medium">{item.stats.total_streams}</p>
                                    </div>
                                  )}
                                  {item.stats.dead_streams !== undefined && (
                                    <div>
                                      <p className="text-xs text-muted-foreground">Dead</p>
                                      <p className="text-sm font-medium text-destructive">{item.stats.dead_streams}</p>
                                    </div>
                                  )}
                                  {item.stats.avg_resolution && (
                                    <div>
                                      <p className="text-xs text-muted-foreground">Avg Res</p>
                                      <p className="text-sm font-medium">{item.stats.avg_resolution}</p>
                                    </div>
                                  )}
                                  {item.stats.avg_bitrate && (
                                    <div>
                                      <p className="text-xs text-muted-foreground">Avg Bitrate</p>
                                      <p className="text-sm font-medium">{item.stats.avg_bitrate}</p>
                                    </div>
                                  )}
                                </div>
                              )}
                            </div>
                          </CardContent>
                        </Card>
                      ))}
                    </div>
                  </AccordionContent>
                </AccordionItem>
              )
            })}
          </Accordion>
        </CardContent>
      )}
    </Card>
  )
}

export default function Changelog() {
  const [entries, setEntries] = useState([])
  const [loading, setLoading] = useState(true)
  const [days, setDays] = useState(7)
  const { toast } = useToast()

  useEffect(() => {
    loadChangelog()
  }, [days])

  const loadChangelog = async () => {
    try {
      setLoading(true)
      const response = await changelogAPI.getChangelog(days)
      setEntries(response.data || [])
    } catch (err) {
      console.error('Failed to load changelog:', err)
      toast({
        title: "Error",
        description: "Failed to load changelog",
        variant: "destructive"
      })
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Changelog</h1>
          <p className="text-muted-foreground">
            View activity history and system events
          </p>
        </div>
        
        <select
          value={days}
          onChange={(e) => setDays(Number(e.target.value))}
          className="px-3 py-2 border rounded-md bg-background"
        >
          <option value={1}>Last 24 hours</option>
          <option value={7}>Last 7 days</option>
          <option value={30}>Last 30 days</option>
          <option value={90}>Last 90 days</option>
        </select>
      </div>

      {loading ? (
        <Card>
          <CardContent className="flex items-center justify-center py-12">
            <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
          </CardContent>
        </Card>
      ) : entries.length === 0 ? (
        <Card>
          <CardContent className="flex flex-col items-center justify-center py-12">
            <Activity className="h-12 w-12 text-muted-foreground mb-4" />
            <p className="text-muted-foreground">No changelog entries found for the selected period</p>
          </CardContent>
        </Card>
      ) : (
        <div className="space-y-4">
          {entries.map((entry, index) => (
            <ChangelogEntry key={index} entry={entry} />
          ))}
        </div>
      )}
    </div>
  )
}
