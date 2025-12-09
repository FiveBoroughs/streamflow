import { useState, useEffect } from 'react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card.jsx'
import { Button } from '@/components/ui/button.jsx'
import { Badge } from '@/components/ui/badge.jsx'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table.jsx'
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle, DialogTrigger } from '@/components/ui/dialog.jsx'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select.jsx'
import { Input } from '@/components/ui/input.jsx'
import { Label } from '@/components/ui/label.jsx'
import { useToast } from '@/hooks/use-toast.js'
import { schedulingAPI, channelsAPI } from '@/services/api.js'
import { Plus, Trash2, Clock, Calendar, RefreshCw, Loader2, Settings } from 'lucide-react'

export default function Scheduling() {
  const [events, setEvents] = useState([])
  const [channels, setChannels] = useState([])
  const [programs, setPrograms] = useState([])
  const [config, setConfig] = useState(null)
  const [loading, setLoading] = useState(true)
  const [loadingPrograms, setLoadingPrograms] = useState(false)
  const [dialogOpen, setDialogOpen] = useState(false)
  const [configDialogOpen, setConfigDialogOpen] = useState(false)
  const [selectedChannel, setSelectedChannel] = useState(null)
  const [selectedProgram, setSelectedProgram] = useState(null)
  const [minutesBefore, setMinutesBefore] = useState(5)
  const [refreshInterval, setRefreshInterval] = useState(60)
  const { toast } = useToast()

  useEffect(() => {
    loadData()
  }, [])

  const loadData = async () => {
    try {
      setLoading(true)
      const [eventsResponse, channelsResponse, configResponse] = await Promise.all([
        schedulingAPI.getEvents(),
        channelsAPI.getChannels(),
        schedulingAPI.getConfig()
      ])
      
      setEvents(eventsResponse.data || [])
      setChannels(channelsResponse.data || [])
      setConfig(configResponse.data || {})
      setRefreshInterval(configResponse.data?.epg_refresh_interval_minutes || 60)
    } catch (err) {
      console.error('Failed to load scheduling data:', err)
      toast({
        title: "Error",
        description: "Failed to load scheduling data",
        variant: "destructive"
      })
    } finally {
      setLoading(false)
    }
  }

  const loadPrograms = async (channelId) => {
    if (!channelId) return
    
    try {
      setLoadingPrograms(true)
      const response = await schedulingAPI.getChannelPrograms(channelId)
      setPrograms(response.data || [])
    } catch (err) {
      console.error('Failed to load programs:', err)
      toast({
        title: "Error",
        description: "Failed to load programs for channel",
        variant: "destructive"
      })
      setPrograms([])
    } finally {
      setLoadingPrograms(false)
    }
  }

  const handleChannelSelect = (channelId) => {
    const channel = channels.find(c => c.id === parseInt(channelId))
    setSelectedChannel(channel)
    setSelectedProgram(null)
    setPrograms([])
    if (channel) {
      loadPrograms(channel.id)
    }
  }

  const handleCreateEvent = async () => {
    if (!selectedChannel || !selectedProgram) {
      toast({
        title: "Validation Error",
        description: "Please select a channel and program",
        variant: "destructive"
      })
      return
    }

    const minutesBeforeValue = parseInt(minutesBefore)
    if (isNaN(minutesBeforeValue) || minutesBeforeValue < 0) {
      toast({
        title: "Validation Error",
        description: "Please enter a valid number of minutes (0 or greater)",
        variant: "destructive"
      })
      return
    }

    try {
      const eventData = {
        channel_id: selectedChannel.id,
        program_start_time: selectedProgram.start_time,
        program_end_time: selectedProgram.end_time,
        program_title: selectedProgram.title,
        minutes_before: minutesBeforeValue
      }

      await schedulingAPI.createEvent(eventData)
      
      toast({
        title: "Success",
        description: "Scheduled event created successfully"
      })

      setDialogOpen(false)
      setSelectedChannel(null)
      setSelectedProgram(null)
      setPrograms([])
      setMinutesBefore(5)
      await loadData()
    } catch (err) {
      console.error('Failed to create event:', err)
      toast({
        title: "Error",
        description: err.response?.data?.error || "Failed to create scheduled event",
        variant: "destructive"
      })
    }
  }

  const handleDeleteEvent = async (eventId) => {
    try {
      await schedulingAPI.deleteEvent(eventId)
      toast({
        title: "Success",
        description: "Scheduled event deleted"
      })
      await loadData()
    } catch (err) {
      console.error('Failed to delete event:', err)
      toast({
        title: "Error",
        description: "Failed to delete scheduled event",
        variant: "destructive"
      })
    }
  }

  const handleUpdateConfig = async () => {
    try {
      await schedulingAPI.updateConfig({
        epg_refresh_interval_minutes: parseInt(refreshInterval)
      })
      
      toast({
        title: "Success",
        description: "Configuration updated successfully"
      })
      
      setConfigDialogOpen(false)
      await loadData()
    } catch (err) {
      console.error('Failed to update config:', err)
      toast({
        title: "Error",
        description: "Failed to update configuration",
        variant: "destructive"
      })
    }
  }

  const formatDateTime = (dateStr) => {
    if (!dateStr) return 'N/A'
    try {
      const date = new Date(dateStr)
      return date.toLocaleString()
    } catch {
      return dateStr
    }
  }

  const formatTime = (dateStr) => {
    if (!dateStr) return 'N/A'
    try {
      const date = new Date(dateStr)
      return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
    } catch {
      return dateStr
    }
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="h-8 w-8 animate-spin text-primary" />
      </div>
    )
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold">Scheduling</h1>
          <p className="text-muted-foreground mt-1">
            Schedule channel checks before EPG events
          </p>
        </div>
        <div className="flex gap-2">
          <Dialog open={configDialogOpen} onOpenChange={setConfigDialogOpen}>
            <DialogTrigger asChild>
              <Button variant="outline" size="sm">
                <Settings className="h-4 w-4 mr-2" />
                Settings
              </Button>
            </DialogTrigger>
            <DialogContent>
              <DialogHeader>
                <DialogTitle>Scheduling Configuration</DialogTitle>
                <DialogDescription>
                  Configure EPG data refresh interval
                </DialogDescription>
              </DialogHeader>
              <div className="space-y-4 py-4">
                <div className="space-y-2">
                  <Label htmlFor="refresh-interval">EPG Refresh Interval (minutes)</Label>
                  <Input
                    id="refresh-interval"
                    type="number"
                    min="1"
                    max="1440"
                    value={refreshInterval}
                    onChange={(e) => setRefreshInterval(e.target.value)}
                  />
                  <p className="text-sm text-muted-foreground">
                    How often to fetch fresh EPG data from Dispatcharr
                  </p>
                </div>
              </div>
              <DialogFooter>
                <Button variant="outline" onClick={() => setConfigDialogOpen(false)}>
                  Cancel
                </Button>
                <Button onClick={handleUpdateConfig}>
                  Save Changes
                </Button>
              </DialogFooter>
            </DialogContent>
          </Dialog>

          <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
            <DialogTrigger asChild>
              <Button>
                <Plus className="h-4 w-4 mr-2" />
                Add Event Check
              </Button>
            </DialogTrigger>
            <DialogContent className="max-w-3xl max-h-[90vh] overflow-y-auto">
              <DialogHeader>
                <DialogTitle>Schedule Channel Check</DialogTitle>
                <DialogDescription>
                  Select a channel, program, and how many minutes before the program starts you want the check to happen
                </DialogDescription>
              </DialogHeader>
              
              <div className="space-y-4 py-4">
                {/* Channel Selection */}
                <div className="space-y-2">
                  <Label htmlFor="channel-select">Channel</Label>
                  <Select onValueChange={handleChannelSelect}>
                    <SelectTrigger id="channel-select">
                      <SelectValue placeholder="Search and select a channel..." />
                    </SelectTrigger>
                    <SelectContent>
                      {channels.map((channel) => (
                        <SelectItem key={channel.id} value={channel.id.toString()}>
                          {channel.channel_number ? `${channel.channel_number} - ` : ''}{channel.name}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>

                {/* Programs List */}
                {selectedChannel && (
                  <div className="space-y-2">
                    <Label>Programs</Label>
                    {loadingPrograms ? (
                      <div className="flex items-center justify-center p-8">
                        <Loader2 className="h-6 w-6 animate-spin text-primary" />
                      </div>
                    ) : programs.length === 0 ? (
                      <div className="text-center text-muted-foreground p-8 border rounded-lg">
                        No programs available for this channel
                      </div>
                    ) : (
                      <div className="border rounded-lg max-h-64 overflow-y-auto">
                        {programs.map((program) => (
                          <div
                            key={program.id || `${program.start_time}-${program.title}`}
                            className={`p-3 border-b last:border-b-0 cursor-pointer hover:bg-muted/50 transition-colors ${
                              selectedProgram?.id === program.id ? 'bg-primary/10' : ''
                            }`}
                            onClick={() => setSelectedProgram(program)}
                          >
                            <div className="flex items-start justify-between">
                              <div className="flex-1">
                                <div className="font-medium">{program.title}</div>
                                <div className="text-sm text-muted-foreground mt-1">
                                  {formatTime(program.start_time)} - {formatTime(program.end_time)}
                                </div>
                                {program.description && (
                                  <div className="text-sm text-muted-foreground mt-1 line-clamp-2">
                                    {program.description}
                                  </div>
                                )}
                              </div>
                            </div>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                )}

                {/* Minutes Before Input */}
                {selectedProgram && (
                  <div className="space-y-2">
                    <Label htmlFor="minutes-before">Minutes Before Program Start</Label>
                    <Input
                      id="minutes-before"
                      type="number"
                      min="0"
                      max="120"
                      value={minutesBefore}
                      onChange={(e) => setMinutesBefore(e.target.value)}
                      placeholder="5"
                    />
                    <p className="text-sm text-muted-foreground">
                      The channel check will run {minutesBefore || 0} minutes before the program starts
                    </p>
                  </div>
                )}
              </div>

              <DialogFooter>
                <Button variant="outline" onClick={() => {
                  setDialogOpen(false)
                  setSelectedChannel(null)
                  setSelectedProgram(null)
                  setPrograms([])
                }}>
                  Cancel
                </Button>
                <Button 
                  onClick={handleCreateEvent}
                  disabled={!selectedChannel || !selectedProgram}
                >
                  Schedule Check
                </Button>
              </DialogFooter>
            </DialogContent>
          </Dialog>
        </div>
      </div>

      {/* Scheduled Events Table */}
      <Card>
        <CardHeader>
          <CardTitle>Scheduled Events</CardTitle>
          <CardDescription>
            Channel checks scheduled before EPG program events
          </CardDescription>
        </CardHeader>
        <CardContent>
          {events.length === 0 ? (
            <div className="text-center text-muted-foreground py-12">
              <Calendar className="h-12 w-12 mx-auto mb-4 opacity-50" />
              <p className="text-lg font-medium mb-2">No scheduled events yet</p>
              <p>Click "Add Event Check" to schedule a channel check before a program</p>
            </div>
          ) : (
            <div className="rounded-md border">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Channel</TableHead>
                    <TableHead>Program</TableHead>
                    <TableHead>Program Time</TableHead>
                    <TableHead>Check Time</TableHead>
                    <TableHead>Minutes Before</TableHead>
                    <TableHead className="text-right">Actions</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {events.map((event) => (
                    <TableRow key={event.id}>
                      <TableCell>
                        <div className="flex items-center gap-2">
                          {event.channel_logo_url && (
                            <img
                              src={event.channel_logo_url}
                              alt={event.channel_name}
                              className="h-8 w-8 object-contain rounded"
                            />
                          )}
                          <span className="font-medium">{event.channel_name}</span>
                        </div>
                      </TableCell>
                      <TableCell>{event.program_title}</TableCell>
                      <TableCell>
                        <div className="flex items-center gap-1 text-sm">
                          <Clock className="h-3 w-3" />
                          {formatTime(event.program_start_time)}
                        </div>
                      </TableCell>
                      <TableCell>
                        <Badge variant="outline">
                          {formatDateTime(event.check_time)}
                        </Badge>
                      </TableCell>
                      <TableCell>
                        {event.minutes_before} min
                      </TableCell>
                      <TableCell className="text-right">
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => {
                            if (window.confirm('Are you sure you want to delete this scheduled event?')) {
                              handleDeleteEvent(event.id)
                            }
                          }}
                        >
                          <Trash2 className="h-4 w-4 text-destructive" />
                        </Button>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Info Card */}
      <Card>
        <CardHeader>
          <CardTitle>How It Works</CardTitle>
        </CardHeader>
        <CardContent className="space-y-2 text-sm text-muted-foreground">
          <p>
            • EPG data is fetched from Dispatcharr every {config?.epg_refresh_interval_minutes || 60} minutes
          </p>
          <p>
            • Schedule channel checks to happen before important programs
          </p>
          <p>
            • A playlist update will also happen right before the scheduled check
          </p>
          <p>
            • This ensures channels have the freshest streams for optimal viewing experience
          </p>
        </CardContent>
      </Card>
    </div>
  )
}
