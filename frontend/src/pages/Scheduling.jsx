import { useState, useEffect, useMemo } from 'react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card.jsx'
import { Button } from '@/components/ui/button.jsx'
import { Badge } from '@/components/ui/badge.jsx'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table.jsx'
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle, DialogTrigger } from '@/components/ui/dialog.jsx'
import { Input } from '@/components/ui/input.jsx'
import { Label } from '@/components/ui/label.jsx'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover.jsx'
import { Command, CommandEmpty, CommandGroup, CommandInput, CommandItem, CommandList } from '@/components/ui/command.jsx'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select.jsx'
import { Pagination, PaginationContent, PaginationItem, PaginationLink, PaginationNext, PaginationPrevious, PaginationEllipsis } from '@/components/ui/pagination.jsx'
import { useToast } from '@/hooks/use-toast.js'
import { schedulingAPI, channelsAPI } from '@/services/api.js'
import { Plus, Trash2, Clock, Calendar, RefreshCw, Loader2, Settings, ChevronsUpDown, Check } from 'lucide-react'
import { cn } from '@/lib/utils.js'

export default function Scheduling() {
  const [events, setEvents] = useState([])
  const [channels, setChannels] = useState([])
  const [programs, setPrograms] = useState([])
  const [config, setConfig] = useState(null)
  const [loading, setLoading] = useState(true)
  const [loadingPrograms, setLoadingPrograms] = useState(false)
  const [dialogOpen, setDialogOpen] = useState(false)
  const [configDialogOpen, setConfigDialogOpen] = useState(false)
  const [channelComboboxOpen, setChannelComboboxOpen] = useState(false)
  const [selectedChannel, setSelectedChannel] = useState(null)
  const [selectedProgram, setSelectedProgram] = useState(null)
  const [minutesBefore, setMinutesBefore] = useState(5)
  const [refreshInterval, setRefreshInterval] = useState(60)
  
  // Pagination state for scheduled events
  const [currentPage, setCurrentPage] = useState(1)
  const [eventsPerPage, setEventsPerPage] = useState(10)
  
  // Auto-create rules state
  const [autoCreateRules, setAutoCreateRules] = useState([])
  const [ruleDialogOpen, setRuleDialogOpen] = useState(false)
  const [ruleChannelComboboxOpen, setRuleChannelComboboxOpen] = useState(false)
  const [ruleSelectedChannel, setRuleSelectedChannel] = useState(null)
  const [ruleName, setRuleName] = useState('')
  const [ruleRegexPattern, setRuleRegexPattern] = useState('')
  const [ruleMinutesBefore, setRuleMinutesBefore] = useState(5)
  const [testingRegex, setTestingRegex] = useState(false)
  const [regexMatches, setRegexMatches] = useState([])
  const [deleteRuleDialogOpen, setDeleteRuleDialogOpen] = useState(false)
  const [ruleToDelete, setRuleToDelete] = useState(null)
  
  const { toast } = useToast()

  // Calculate paginated events with useMemo for performance
  const paginationData = useMemo(() => {
    const totalPages = Math.ceil(events.length / eventsPerPage)
    const startIndex = (currentPage - 1) * eventsPerPage
    const endIndex = startIndex + eventsPerPage
    const paginatedEvents = events.slice(startIndex, endIndex)
    
    return {
      totalPages,
      startIndex,
      endIndex,
      paginatedEvents,
      showingStart: startIndex + 1,
      showingEnd: Math.min(endIndex, events.length)
    }
  }, [events, currentPage, eventsPerPage])

  useEffect(() => {
    loadData()
  }, [])

  const loadData = async () => {
    try {
      setLoading(true)
      const [eventsResponse, channelsResponse, configResponse, rulesResponse] = await Promise.all([
        schedulingAPI.getEvents(),
        channelsAPI.getChannels(),
        schedulingAPI.getConfig(),
        schedulingAPI.getAutoCreateRules()
      ])
      
      setEvents(eventsResponse.data || [])
      setChannels(channelsResponse.data || [])
      setConfig(configResponse.data || {})
      setRefreshInterval(configResponse.data?.epg_refresh_interval_minutes || 60)
      setAutoCreateRules(rulesResponse.data || [])
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
    setChannelComboboxOpen(false)
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
      setChannelComboboxOpen(false)
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

  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false)
  const [eventToDelete, setEventToDelete] = useState(null)

  const validateMinutesBefore = (value) => {
    const minutesValue = parseInt(value)
    return !isNaN(minutesValue) && minutesValue >= 0
  }

  const handleRuleChannelSelect = (channelId) => {
    const channel = channels.find(c => c.id === parseInt(channelId))
    setRuleSelectedChannel(channel)
    setRuleChannelComboboxOpen(false)
    // Clear regex matches when channel changes
    setRegexMatches([])
  }

  const handleTestRegex = async () => {
    if (!ruleSelectedChannel || !ruleRegexPattern) {
      toast({
        title: "Validation Error",
        description: "Please select a channel and enter a regex pattern",
        variant: "destructive"
      })
      return
    }

    try {
      setTestingRegex(true)
      const response = await schedulingAPI.testAutoCreateRule({
        channel_id: ruleSelectedChannel.id,
        regex_pattern: ruleRegexPattern
      })
      
      setRegexMatches(response.data.programs || [])
      
      if (response.data.matches === 0) {
        toast({
          title: "No Matches",
          description: "The regex pattern didn't match any programs in the EPG",
          variant: "default"
        })
      }
    } catch (err) {
      console.error('Failed to test regex:', err)
      toast({
        title: "Error",
        description: err.response?.data?.error || "Failed to test regex pattern",
        variant: "destructive"
      })
      setRegexMatches([])
    } finally {
      setTestingRegex(false)
    }
  }

  const handleCreateRule = async () => {
    if (!ruleName || !ruleSelectedChannel || !ruleRegexPattern) {
      toast({
        title: "Validation Error",
        description: "Please fill in all required fields",
        variant: "destructive"
      })
      return
    }

    // Parse once and validate
    const minutesBeforeValue = parseInt(ruleMinutesBefore)
    if (!validateMinutesBefore(ruleMinutesBefore)) {
      toast({
        title: "Validation Error",
        description: "Please enter a valid number of minutes (0 or greater)",
        variant: "destructive"
      })
      return
    }

    try {
      const ruleData = {
        name: ruleName,
        channel_id: ruleSelectedChannel.id,
        regex_pattern: ruleRegexPattern,
        minutes_before: minutesBeforeValue
      }

      await schedulingAPI.createAutoCreateRule(ruleData)
      
      toast({
        title: "Success",
        description: "Auto-create rule created successfully. Events will be created automatically when EPG is refreshed."
      })

      setRuleDialogOpen(false)
      setRuleName('')
      setRuleSelectedChannel(null)
      setRuleRegexPattern('')
      setRuleMinutesBefore(5)
      setRegexMatches([])
      setRuleChannelComboboxOpen(false)
      await loadData()
    } catch (err) {
      console.error('Failed to create rule:', err)
      toast({
        title: "Error",
        description: err.response?.data?.error || "Failed to create auto-create rule",
        variant: "destructive"
      })
    }
  }

  const handleDeleteRule = async (ruleId) => {
    try {
      await schedulingAPI.deleteAutoCreateRule(ruleId)
      toast({
        title: "Success",
        description: "Auto-create rule deleted"
      })
      await loadData()
      setDeleteRuleDialogOpen(false)
      setRuleToDelete(null)
    } catch (err) {
      console.error('Failed to delete rule:', err)
      toast({
        title: "Error",
        description: "Failed to delete auto-create rule",
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
      setDeleteDialogOpen(false)
      setEventToDelete(null)
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
                  <Popover open={channelComboboxOpen} onOpenChange={setChannelComboboxOpen}>
                    <PopoverTrigger asChild>
                      <Button
                        variant="outline"
                        role="combobox"
                        aria-expanded={channelComboboxOpen}
                        className="w-full justify-between"
                      >
                        {selectedChannel
                          ? `${selectedChannel.channel_number ? `${selectedChannel.channel_number} - ` : ''}${selectedChannel.name}`
                          : "Search and select a channel..."}
                        <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
                      </Button>
                    </PopoverTrigger>
                    <PopoverContent className="w-[500px] p-0" align="start">
                      <Command>
                        <CommandInput placeholder="Search channels..." className="h-9" />
                        <CommandList>
                          <CommandEmpty>No channel found.</CommandEmpty>
                          <CommandGroup>
                            {channels.map((channel) => {
                              const channelNumber = channel.channel_number ? `${channel.channel_number} ` : '';
                              const searchValue = `${channelNumber}${channel.name}`.toLowerCase().trim();
                              return (
                              <CommandItem
                                key={channel.id}
                                value={searchValue}
                                onSelect={() => handleChannelSelect(channel.id)}
                              >
                                {channel.channel_number ? `${channel.channel_number} - ` : ''}{channel.name}
                                <Check
                                  className={cn(
                                    "ml-auto h-4 w-4",
                                    selectedChannel?.id === channel.id ? "opacity-100" : "opacity-0"
                                  )}
                                />
                              </CommandItem>
                              );
                            })}
                          </CommandGroup>
                        </CommandList>
                      </Command>
                    </PopoverContent>
                  </Popover>
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
                              selectedProgram?.id === program.id 
                                ? 'bg-primary/10 border-2 border-green-500/50 dark:border-green-400/50' 
                                : ''
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
                  setChannelComboboxOpen(false)
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
          <div className="flex items-center justify-between">
            <div>
              <CardTitle>Scheduled Events</CardTitle>
              <CardDescription>
                Channel checks scheduled before EPG program events
              </CardDescription>
            </div>
            {events.length > 0 && (
              <div className="flex items-center gap-2">
                <Label htmlFor="events-per-page" className="text-sm whitespace-nowrap">
                  Events per page:
                </Label>
                <Select
                  value={eventsPerPage.toString()}
                  onValueChange={(value) => {
                    setEventsPerPage(parseInt(value))
                    setCurrentPage(1) // Reset to first page when changing page size
                  }}
                >
                  <SelectTrigger id="events-per-page" className="w-[100px]">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="10">10</SelectItem>
                    <SelectItem value="25">25</SelectItem>
                    <SelectItem value="50">50</SelectItem>
                    <SelectItem value="100">100</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            )}
          </div>
        </CardHeader>
        <CardContent>
          {events.length === 0 ? (
            <div className="text-center text-muted-foreground py-12">
              <Calendar className="h-12 w-12 mx-auto mb-4 opacity-50" />
              <p className="text-lg font-medium mb-2">No scheduled events yet</p>
              <p>Click "Add Event Check" to schedule a channel check before a program</p>
            </div>
          ) : (
            <>
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
                    {paginationData.paginatedEvents.map((event) => (
                        <TableRow key={event.id}>
                          <TableCell>
                            <div className="flex items-center gap-2">
                              {event.channel_logo_url && (
                                <img
                                  src={event.channel_logo_url}
                                  alt={event.channel_name}
                                  className="h-8 w-8 object-contain rounded"
                                  onError={(e) => { e.target.style.display = 'none' }}
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
                                setEventToDelete(event.id)
                                setDeleteDialogOpen(true)
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
              
              {/* Pagination Controls */}
              {paginationData.totalPages > 1 && (
                <div className="flex items-center justify-between mt-4">
                  <div className="text-sm text-muted-foreground">
                    Showing {paginationData.showingStart} to {paginationData.showingEnd} of {events.length} events
                  </div>
                  <Pagination>
                    <PaginationContent>
                      <PaginationItem>
                        <PaginationPrevious 
                          onClick={() => setCurrentPage(prev => Math.max(1, prev - 1))}
                          className={currentPage === 1 ? 'pointer-events-none opacity-50' : 'cursor-pointer'}
                        />
                      </PaginationItem>
                      
                      {(() => {
                        const { totalPages } = paginationData
                        const pages = []
                        
                        // For 2 or fewer pages, show all pages
                        if (totalPages <= 2) {
                          for (let i = 1; i <= totalPages; i++) {
                            pages.push(
                              <PaginationItem key={i}>
                                <PaginationLink
                                  onClick={() => setCurrentPage(i)}
                                  isActive={currentPage === i}
                                  className="cursor-pointer"
                                >
                                  {i}
                                </PaginationLink>
                              </PaginationItem>
                            )
                          }
                        } else {
                          // Always show first page
                          pages.push(
                            <PaginationItem key={1}>
                              <PaginationLink
                                onClick={() => setCurrentPage(1)}
                                isActive={currentPage === 1}
                                className="cursor-pointer"
                              >
                                1
                              </PaginationLink>
                            </PaginationItem>
                          )
                          
                          // Show ellipsis if needed
                          if (currentPage > 3) {
                            pages.push(
                              <PaginationItem key="ellipsis-1">
                                <PaginationEllipsis />
                              </PaginationItem>
                            )
                          }
                          
                          // Show pages around current page
                          const startPage = Math.max(2, currentPage - 1)
                          const endPage = Math.min(totalPages - 1, currentPage + 1)
                          
                          for (let i = startPage; i <= endPage; i++) {
                            pages.push(
                              <PaginationItem key={i}>
                                <PaginationLink
                                  onClick={() => setCurrentPage(i)}
                                  isActive={currentPage === i}
                                  className="cursor-pointer"
                                >
                                  {i}
                                </PaginationLink>
                              </PaginationItem>
                            )
                          }
                          
                          // Show ellipsis if needed
                          if (currentPage < totalPages - 2) {
                            pages.push(
                              <PaginationItem key="ellipsis-2">
                                <PaginationEllipsis />
                              </PaginationItem>
                            )
                          }
                          
                          // Always show last page
                          pages.push(
                            <PaginationItem key={totalPages}>
                              <PaginationLink
                                onClick={() => setCurrentPage(totalPages)}
                                isActive={currentPage === totalPages}
                                className="cursor-pointer"
                              >
                                {totalPages}
                              </PaginationLink>
                            </PaginationItem>
                          )
                        }
                        
                        return pages
                      })()}
                      
                      <PaginationItem>
                        <PaginationNext 
                          onClick={() => setCurrentPage(prev => Math.min(paginationData.totalPages, prev + 1))}
                          className={currentPage === paginationData.totalPages ? 'pointer-events-none opacity-50' : 'cursor-pointer'}
                        />
                      </PaginationItem>
                    </PaginationContent>
                  </Pagination>
                </div>
              )}
            </>
          )}
        </CardContent>
      </Card>

      {/* Auto-Create Rules Card */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle>Auto-Create Rules</CardTitle>
              <CardDescription>
                Automatically create scheduled events based on regex patterns matching EPG program names
              </CardDescription>
            </div>
            <Dialog open={ruleDialogOpen} onOpenChange={setRuleDialogOpen}>
              <DialogTrigger asChild>
                <Button size="sm">
                  <Plus className="h-4 w-4 mr-2" />
                  Add Rule
                </Button>
              </DialogTrigger>
              <DialogContent className="max-w-4xl max-h-[90vh] overflow-y-auto">
                <DialogHeader>
                  <DialogTitle>Create Auto-Create Rule</DialogTitle>
                  <DialogDescription>
                    Define a regex pattern to automatically create scheduled checks for matching programs
                  </DialogDescription>
                </DialogHeader>
                
                <div className="space-y-4 py-4">
                  {/* Rule Name */}
                  <div className="space-y-2">
                    <Label htmlFor="rule-name">Rule Name</Label>
                    <Input
                      id="rule-name"
                      placeholder="e.g., Breaking News Alert"
                      value={ruleName}
                      onChange={(e) => setRuleName(e.target.value)}
                    />
                  </div>

                  {/* Channel Selection */}
                  <div className="space-y-2">
                    <Label htmlFor="rule-channel-select">Channel</Label>
                    <Popover open={ruleChannelComboboxOpen} onOpenChange={setRuleChannelComboboxOpen}>
                      <PopoverTrigger asChild>
                        <Button
                          variant="outline"
                          role="combobox"
                          aria-expanded={ruleChannelComboboxOpen}
                          className="w-full justify-between"
                        >
                          {ruleSelectedChannel
                            ? `${ruleSelectedChannel.channel_number ? `${ruleSelectedChannel.channel_number} - ` : ''}${ruleSelectedChannel.name}`
                            : "Search and select a channel..."}
                          <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
                        </Button>
                      </PopoverTrigger>
                      <PopoverContent className="w-[600px] p-0" align="start">
                        <Command>
                          <CommandInput placeholder="Search channels..." className="h-9" />
                          <CommandList>
                            <CommandEmpty>No channel found.</CommandEmpty>
                            <CommandGroup>
                              {channels.map((channel) => {
                                const channelNumber = channel.channel_number ? `${channel.channel_number} ` : '';
                                const searchValue = `${channelNumber}${channel.name}`.toLowerCase().trim();
                                return (
                                <CommandItem
                                  key={channel.id}
                                  value={searchValue}
                                  onSelect={() => handleRuleChannelSelect(channel.id)}
                                >
                                  {channel.channel_number ? `${channel.channel_number} - ` : ''}{channel.name}
                                  <Check
                                    className={cn(
                                      "ml-auto h-4 w-4",
                                      ruleSelectedChannel?.id === channel.id ? "opacity-100" : "opacity-0"
                                    )}
                                  />
                                </CommandItem>
                                );
                              })}
                            </CommandGroup>
                          </CommandList>
                        </Command>
                      </PopoverContent>
                    </Popover>
                  </div>

                  {/* Regex Pattern */}
                  {ruleSelectedChannel && (
                    <>
                      <div className="space-y-2">
                        <div className="flex items-center justify-between">
                          <Label htmlFor="rule-regex">Regex Pattern</Label>
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={handleTestRegex}
                            disabled={!ruleRegexPattern || testingRegex}
                          >
                            {testingRegex ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <RefreshCw className="h-4 w-4 mr-2" />}
                            Test Pattern
                          </Button>
                        </div>
                        <Input
                          id="rule-regex"
                          placeholder="e.g., ^Breaking News|^Special Report"
                          value={ruleRegexPattern}
                          onChange={(e) => {
                            setRuleRegexPattern(e.target.value)
                            setRegexMatches([])  // Clear matches when pattern changes
                          }}
                        />
                        <p className="text-sm text-muted-foreground">
                          Use regex syntax to match program titles. Click "Test Pattern" to see live results.
                        </p>
                      </div>

                      {/* Live Regex Results */}
                      {regexMatches.length > 0 && (
                        <div className="space-y-2">
                          <Label>Matching Programs ({regexMatches.length})</Label>
                          <div className="border rounded-lg max-h-48 overflow-y-auto">
                            {regexMatches.map((program, idx) => (
                              <div
                                key={idx}
                                className="p-2 border-b last:border-b-0 text-sm"
                              >
                                <div className="font-medium">{program.title}</div>
                                <div className="text-muted-foreground text-xs mt-1">
                                  {formatTime(program.start_time)} - {formatTime(program.end_time)}
                                </div>
                              </div>
                            ))}
                          </div>
                        </div>
                      )}

                      {/* Minutes Before Input */}
                      <div className="space-y-2">
                        <Label htmlFor="rule-minutes-before">Minutes Before Program Start</Label>
                        <Input
                          id="rule-minutes-before"
                          type="number"
                          min="0"
                          max="120"
                          value={ruleMinutesBefore}
                          onChange={(e) => setRuleMinutesBefore(e.target.value)}
                        />
                        <p className="text-sm text-muted-foreground">
                          Channel checks will run {ruleMinutesBefore || 0} minutes before matching programs start
                        </p>
                      </div>
                    </>
                  )}
                </div>

                <DialogFooter>
                  <Button variant="outline" onClick={() => {
                    setRuleDialogOpen(false)
                    setRuleName('')
                    setRuleSelectedChannel(null)
                    setRuleRegexPattern('')
                    setRuleMinutesBefore(5)
                    setRegexMatches([])
                    setRuleChannelComboboxOpen(false)
                  }}>
                    Cancel
                  </Button>
                  <Button 
                    onClick={handleCreateRule}
                    disabled={!ruleName || !ruleSelectedChannel || !ruleRegexPattern}
                  >
                    Create Rule
                  </Button>
                </DialogFooter>
              </DialogContent>
            </Dialog>
          </div>
        </CardHeader>
        <CardContent>
          {autoCreateRules.length === 0 ? (
            <div className="text-center text-muted-foreground py-8">
              <Settings className="h-10 w-10 mx-auto mb-3 opacity-50" />
              <p className="text-base font-medium mb-1">No auto-create rules yet</p>
              <p className="text-sm">Click "Add Rule" to automatically create events based on program names</p>
            </div>
          ) : (
            <div className="rounded-md border">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Rule Name</TableHead>
                    <TableHead>Channel</TableHead>
                    <TableHead>Regex Pattern</TableHead>
                    <TableHead>Minutes Before</TableHead>
                    <TableHead className="text-right">Actions</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {autoCreateRules.map((rule) => (
                    <TableRow key={rule.id}>
                      <TableCell className="font-medium">{rule.name}</TableCell>
                      <TableCell>
                        <div className="flex items-center gap-2">
                          {rule.channel_logo_url && (
                            <img
                              src={rule.channel_logo_url}
                              alt={rule.channel_name}
                              className="h-6 w-6 object-contain rounded"
                              onError={(e) => { e.target.style.display = 'none' }}
                            />
                          )}
                          <span>{rule.channel_name}</span>
                        </div>
                      </TableCell>
                      <TableCell>
                        <code className="text-xs bg-muted px-2 py-1 rounded">{rule.regex_pattern}</code>
                      </TableCell>
                      <TableCell>{rule.minutes_before} min</TableCell>
                      <TableCell className="text-right">
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => {
                            setRuleToDelete(rule.id)
                            setDeleteRuleDialogOpen(true)
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

      {/* Delete Confirmation Dialog */}
      <Dialog open={deleteDialogOpen} onOpenChange={setDeleteDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete Scheduled Event</DialogTitle>
            <DialogDescription>
              Are you sure you want to delete this scheduled event? This action cannot be undone.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => {
                setDeleteDialogOpen(false)
                setEventToDelete(null)
              }}
            >
              Cancel
            </Button>
            <Button
              variant="destructive"
              onClick={() => handleDeleteEvent(eventToDelete)}
            >
              Delete
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Delete Rule Confirmation Dialog */}
      <Dialog open={deleteRuleDialogOpen} onOpenChange={setDeleteRuleDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete Auto-Create Rule</DialogTitle>
            <DialogDescription>
              Are you sure you want to delete this rule? This will also delete all scheduled events that were automatically created by this rule.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => {
                setDeleteRuleDialogOpen(false)
                setRuleToDelete(null)
              }}
            >
              Cancel
            </Button>
            <Button
              variant="destructive"
              onClick={() => handleDeleteRule(ruleToDelete)}
            >
              Delete
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}
