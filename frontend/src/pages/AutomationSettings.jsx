import { useState, useEffect } from 'react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card.jsx'
import { Button } from '@/components/ui/button.jsx'
import { Input } from '@/components/ui/input.jsx'
import { Label } from '@/components/ui/label.jsx'
import { Switch } from '@/components/ui/switch.jsx'
import { RadioGroup, RadioGroupItem } from '@/components/ui/radio-group.jsx'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs.jsx'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert.jsx'
import { Loader2, AlertCircle, CheckCircle2 } from 'lucide-react'
import { useToast } from '@/hooks/use-toast.js'
import { automationAPI, streamCheckerAPI, dispatcharrAPI } from '@/services/api.js'

export default function AutomationSettings() {
  const [config, setConfig] = useState(null)
  const [streamCheckerConfig, setStreamCheckerConfig] = useState(null)
  const [dispatcharrConfig, setDispatcharrConfig] = useState(null)
  const [testingConnection, setTestingConnection] = useState(false)
  const [connectionTestResult, setConnectionTestResult] = useState(null)
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const { toast } = useToast()

  useEffect(() => {
    loadConfig()
  }, [])

  const loadConfig = async () => {
    try {
      setLoading(true)
      const [automationResponse, streamCheckerResponse, dispatcharrResponse] = await Promise.all([
        automationAPI.getConfig(),
        streamCheckerAPI.getConfig(),
        dispatcharrAPI.getConfig()
      ])
      setConfig(automationResponse.data)
      setStreamCheckerConfig(streamCheckerResponse.data)
      setDispatcharrConfig(dispatcharrResponse.data)
    } catch (err) {
      console.error('Failed to load config:', err)
      toast({
        title: "Error",
        description: "Failed to load automation configuration",
        variant: "destructive"
      })
    } finally {
      setLoading(false)
    }
  }

  const handleSave = async () => {
    try {
      setSaving(true)
      await Promise.all([
        automationAPI.updateConfig(config),
        streamCheckerAPI.updateConfig(streamCheckerConfig),
        dispatcharrAPI.updateConfig(dispatcharrConfig)
      ])
      toast({
        title: "Success",
        description: "Configuration saved successfully",
      })
    } catch (err) {
      toast({
        title: "Error",
        description: "Failed to save configuration",
        variant: "destructive"
      })
    } finally {
      setSaving(false)
    }
  }

  const handleConfigChange = (field, value) => {
    if (field.includes('.')) {
      const [parent, child] = field.split('.')
      setConfig(prev => ({
        ...prev,
        [parent]: {
          ...prev[parent],
          [child]: value
        }
      }))
    } else {
      setConfig(prev => ({
        ...prev,
        [field]: value
      }))
    }
  }

  const handleStreamCheckerConfigChange = (field, value) => {
    if (field.includes('.')) {
      const parts = field.split('.')
      if (parts.length === 2) {
        const [parent, child] = parts
        setStreamCheckerConfig(prev => ({
          ...prev,
          [parent]: {
            ...(prev[parent] || {}),
            [child]: value
          }
        }))
      } else if (parts.length === 3) {
        const [parent, child, grandchild] = parts
        setStreamCheckerConfig(prev => ({
          ...prev,
          [parent]: {
            ...(prev[parent] || {}),
            [child]: {
              ...(prev[parent]?.[child] || {}),
              [grandchild]: value
            }
          }
        }))
      }
    } else {
      setStreamCheckerConfig(prev => ({
        ...prev,
        [field]: value
      }))
    }
  }

  const handleDispatcharrConfigChange = (field, value) => {
    setDispatcharrConfig(prev => ({
      ...prev,
      [field]: value
    }))
  }

  const handleTestConnection = async () => {
    try {
      setTestingConnection(true)
      setConnectionTestResult(null)
      
      const response = await dispatcharrAPI.testConnection(dispatcharrConfig)
      setConnectionTestResult({
        success: true,
        message: 'Connection successful!',
        ...response.data
      })
      
      toast({
        title: "Success",
        description: "Successfully connected to Dispatcharr"
      })
    } catch (err) {
      const errorMsg = err.response?.data?.error || 'Failed to connect to Dispatcharr'
      setConnectionTestResult({
        success: false,
        message: errorMsg
      })
      toast({
        title: "Connection Failed",
        description: errorMsg,
        variant: "destructive"
      })
    } finally {
      setTestingConnection(false)
    }
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-[400px]">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (!config || !streamCheckerConfig) {
    return (
      <Alert variant="destructive">
        <AlertCircle className="h-4 w-4" />
        <AlertTitle>Error</AlertTitle>
        <AlertDescription>
          Failed to load configuration
        </AlertDescription>
      </Alert>
    )
  }

  const pipelineMode = streamCheckerConfig?.pipeline_mode
  
  // Determine which settings to show based on pipeline mode
  const showScheduleSettings = ['pipeline_1_5', 'pipeline_2_5', 'pipeline_3'].includes(pipelineMode)
  const showUpdateInterval = ['pipeline_1', 'pipeline_1_5', 'pipeline_2', 'pipeline_2_5'].includes(pipelineMode)

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold tracking-tight">Automation Settings</h1>
        <p className="text-muted-foreground">
          Configure Dispatcharr connection, pipeline mode, scheduling, and automation parameters
        </p>
      </div>

      {/* Dispatcharr Configuration */}
      <Card>
        <CardHeader>
          <CardTitle>Dispatcharr Connection</CardTitle>
          <CardDescription>
            Configure connection settings to the Dispatcharr API
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="space-y-2">
            <Label htmlFor="base_url">Base URL</Label>
            <Input
              id="base_url"
              type="url"
              value={dispatcharrConfig?.base_url || ''}
              onChange={(e) => handleDispatcharrConfigChange('base_url', e.target.value)}
              placeholder="http://localhost:9191"
            />
            <p className="text-sm text-muted-foreground">The base URL for your Dispatcharr instance</p>
          </div>

          <div className="space-y-2">
            <Label htmlFor="username">Username</Label>
            <Input
              id="username"
              type="text"
              value={dispatcharrConfig?.username || ''}
              onChange={(e) => handleDispatcharrConfigChange('username', e.target.value)}
              placeholder="admin"
            />
            <p className="text-sm text-muted-foreground">Your Dispatcharr username</p>
          </div>

          <div className="space-y-2">
            <Label htmlFor="password">Password</Label>
            <Input
              id="password"
              type="password"
              value={dispatcharrConfig?.password || ''}
              onChange={(e) => handleDispatcharrConfigChange('password', e.target.value)}
              placeholder={dispatcharrConfig?.has_password ? '••••••••' : 'Enter password'}
            />
            <p className="text-sm text-muted-foreground">
              {dispatcharrConfig?.has_password ? 'Leave blank to keep existing password' : 'Your Dispatcharr password'}
            </p>
          </div>

          <div className="flex items-center gap-2 pt-2">
            <Button 
              onClick={handleTestConnection} 
              disabled={testingConnection}
              variant="outline"
            >
              {testingConnection && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
              Test Connection
            </Button>
            {connectionTestResult && (
              <div className="flex items-center gap-2">
                {connectionTestResult.success ? (
                  <>
                    <CheckCircle2 className="h-4 w-4 text-green-600" />
                    <span className="text-sm text-green-600">{connectionTestResult.message}</span>
                  </>
                ) : (
                  <>
                    <AlertCircle className="h-4 w-4 text-destructive" />
                    <span className="text-sm text-destructive">{connectionTestResult.message}</span>
                  </>
                )}
              </div>
            )}
          </div>
        </CardContent>
      </Card>

      {/* Pipeline Selection */}
      <Card>
        <CardHeader>
          <CardTitle>Pipeline Selection</CardTitle>
          <CardDescription>
            Select the automation pipeline that best fits your needs. Each pipeline determines when and how streams are checked.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <RadioGroup
            value={pipelineMode}
            onValueChange={(value) => handleStreamCheckerConfigChange('pipeline_mode', value)}
            className="space-y-3"
          >
            {/* Disabled */}
            <Card className={`${pipelineMode === 'disabled' ? 'ring-2 ring-destructive' : ''}`}>
              <CardContent className="pt-6">
                <div className="flex items-start space-x-3">
                  <RadioGroupItem value="disabled" id="disabled" />
                  <div className="flex-1">
                    <Label htmlFor="disabled" className="text-base font-semibold">Disabled</Label>
                    <p className="text-sm text-muted-foreground mt-1">Features:</p>
                    <ul className="text-sm text-muted-foreground list-disc list-inside mt-1 space-y-1">
                      <li>Complete automation system disabled</li>
                      <li>No automatic updates, matching, or checking</li>
                    </ul>
                  </div>
                </div>
              </CardContent>
            </Card>

            {/* Pipeline 1 */}
            <Card className={`${pipelineMode === 'pipeline_1' ? 'ring-2 ring-primary' : ''}`}>
              <CardContent className="pt-6">
                <div className="flex items-start space-x-3">
                  <RadioGroupItem value="pipeline_1" id="pipeline_1" />
                  <div className="flex-1">
                    <Label htmlFor="pipeline_1" className="text-base font-semibold">Pipeline 1: Update → Match → Check (with 2hr immunity)</Label>
                    <p className="text-sm text-muted-foreground mt-1">Features:</p>
                    <ul className="text-sm text-muted-foreground list-disc list-inside mt-1 space-y-1">
                      <li>Automatic M3U updates</li>
                      <li>Stream matching</li>
                      <li>Quality checking with 2-hour immunity</li>
                    </ul>
                  </div>
                </div>
              </CardContent>
            </Card>

            {/* Pipeline 1.5 */}
            <Card className={`${pipelineMode === 'pipeline_1_5' ? 'ring-2 ring-primary' : ''}`}>
              <CardContent className="pt-6">
                <div className="flex items-start space-x-3">
                  <RadioGroupItem value="pipeline_1_5" id="pipeline_1_5" />
                  <div className="flex-1">
                    <Label htmlFor="pipeline_1_5" className="text-base font-semibold">Pipeline 1.5: Pipeline 1 + Scheduled Global Action</Label>
                    <p className="text-sm text-muted-foreground mt-1">Features:</p>
                    <ul className="text-sm text-muted-foreground list-disc list-inside mt-1 space-y-1">
                      <li>Automatic M3U updates</li>
                      <li>Stream matching</li>
                      <li>Quality checking with 2-hour immunity</li>
                      <li>Scheduled Global Action (daily/monthly)</li>
                    </ul>
                  </div>
                </div>
              </CardContent>
            </Card>

            {/* Pipeline 2 */}
            <Card className={`${pipelineMode === 'pipeline_2' ? 'ring-2 ring-primary' : ''}`}>
              <CardContent className="pt-6">
                <div className="flex items-start space-x-3">
                  <RadioGroupItem value="pipeline_2" id="pipeline_2" />
                  <div className="flex-1">
                    <Label htmlFor="pipeline_2" className="text-base font-semibold">Pipeline 2: Update → Match only (no automatic checking)</Label>
                    <p className="text-sm text-muted-foreground mt-1">Features:</p>
                    <ul className="text-sm text-muted-foreground list-disc list-inside mt-1 space-y-1">
                      <li>Automatic M3U updates</li>
                      <li>Stream matching</li>
                    </ul>
                  </div>
                </div>
              </CardContent>
            </Card>

            {/* Pipeline 2.5 */}
            <Card className={`${pipelineMode === 'pipeline_2_5' ? 'ring-2 ring-primary' : ''}`}>
              <CardContent className="pt-6">
                <div className="flex items-start space-x-3">
                  <RadioGroupItem value="pipeline_2_5" id="pipeline_2_5" />
                  <div className="flex-1">
                    <Label htmlFor="pipeline_2_5" className="text-base font-semibold">Pipeline 2.5: Pipeline 2 + Scheduled Global Action</Label>
                    <p className="text-sm text-muted-foreground mt-1">Features:</p>
                    <ul className="text-sm text-muted-foreground list-disc list-inside mt-1 space-y-1">
                      <li>Automatic M3U updates</li>
                      <li>Stream matching</li>
                      <li>Scheduled Global Action (daily/monthly)</li>
                    </ul>
                  </div>
                </div>
              </CardContent>
            </Card>

            {/* Pipeline 3 */}
            <Card className={`${pipelineMode === 'pipeline_3' ? 'ring-2 ring-primary' : ''}`}>
              <CardContent className="pt-6">
                <div className="flex items-start space-x-3">
                  <RadioGroupItem value="pipeline_3" id="pipeline_3" />
                  <div className="flex-1">
                    <Label htmlFor="pipeline_3" className="text-base font-semibold">Pipeline 3: Only Scheduled Global Action</Label>
                    <p className="text-sm text-muted-foreground mt-1">Features:</p>
                    <ul className="text-sm text-muted-foreground list-disc list-inside mt-1 space-y-1">
                      <li>Scheduled Global Action ONLY (daily/monthly)</li>
                      <li>NO automatic updates or checking</li>
                    </ul>
                  </div>
                </div>
              </CardContent>
            </Card>
          </RadioGroup>
        </CardContent>
      </Card>

      {/* No Active Pipeline Warning */}
      {(pipelineMode === null || pipelineMode === undefined || pipelineMode === '') && (
        <Alert variant="warning">
          <AlertCircle className="h-4 w-4" />
          <AlertTitle>No Active Pipeline</AlertTitle>
          <AlertDescription>
            Please select a pipeline above to configure automation settings. All configuration options will appear once a pipeline is selected.
          </AlertDescription>
        </Alert>
      )}

      {/* Disabled Mode Warning */}
      {pipelineMode === 'disabled' && (
        <Alert variant="warning">
          <AlertCircle className="h-4 w-4" />
          <AlertTitle>Automation System Disabled</AlertTitle>
          <AlertDescription>
            The complete automation system is currently disabled. No automatic updates, stream matching, or quality checking will occur. Select a pipeline above to enable automation.
          </AlertDescription>
        </Alert>
      )}

      {/* Update Interval Settings */}
      {showUpdateInterval && (
        <Card>
          <CardHeader>
            <CardTitle>Playlist Update Configuration</CardTitle>
            <CardDescription>
              Configure how often playlists are updated using either interval or cron expression
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <Tabs 
              defaultValue={config.playlist_update_cron ? "cron" : "interval"} 
              onValueChange={(value) => {
                // Clear the other option when switching tabs
                if (value === "interval") {
                  handleConfigChange('playlist_update_cron', '')
                  // Ensure interval has a default value
                  if (!config.playlist_update_interval_minutes) {
                    handleConfigChange('playlist_update_interval_minutes', 5)
                  }
                } else {
                  // Don't clear interval, just switch to using cron
                  // The backend will prioritize cron when both are set
                }
              }}
            >
              <TabsList className="grid w-full grid-cols-2">
                <TabsTrigger value="interval">Interval (Minutes)</TabsTrigger>
                <TabsTrigger value="cron">Cron Expression</TabsTrigger>
              </TabsList>
              
              <TabsContent value="interval" className="space-y-2">
                <Label htmlFor="playlist_update_interval">Playlist Update Interval (minutes)</Label>
                <Input
                  id="playlist_update_interval"
                  type="number"
                  min="1"
                  max="1440"
                  value={config.playlist_update_interval_minutes || 5}
                  onChange={(e) => handleConfigChange('playlist_update_interval_minutes', parseInt(e.target.value))}
                />
                <p className="text-sm text-muted-foreground">How often to check for playlist updates (in minutes)</p>
              </TabsContent>
              
              <TabsContent value="cron" className="space-y-2">
                <Label htmlFor="playlist_update_cron">Cron Expression</Label>
                <Input
                  id="playlist_update_cron"
                  type="text"
                  value={config.playlist_update_cron || ''}
                  onChange={(e) => handleConfigChange('playlist_update_cron', e.target.value)}
                  placeholder="*/30 * * * *"
                />
                <p className="text-sm text-muted-foreground">Use cron expression for more precise scheduling</p>
                <Alert>
                  <AlertCircle className="h-4 w-4" />
                  <AlertTitle>Cron Expression Format</AlertTitle>
                  <AlertDescription>
                    <p className="font-semibold mb-2">Format: minute hour day month weekday</p>
                    <p className="mb-1">Common examples:</p>
                    <ul className="list-disc list-inside space-y-1">
                      <li><code className="text-sm">*/5 * * * *</code> - Every 5 minutes</li>
                      <li><code className="text-sm">*/30 * * * *</code> - Every 30 minutes</li>
                      <li><code className="text-sm">0 * * * *</code> - Every hour</li>
                      <li><code className="text-sm">0 */6 * * *</code> - Every 6 hours</li>
                      <li><code className="text-sm">0 0 * * *</code> - Daily at midnight</li>
                    </ul>
                  </AlertDescription>
                </Alert>
              </TabsContent>
            </Tabs>
          </CardContent>
        </Card>
      )}

      {/* Stream Checker Service Info */}
      {pipelineMode && pipelineMode !== 'disabled' && (
        <Alert>
          <CheckCircle2 className="h-4 w-4" />
          <AlertTitle>Stream Checker Service</AlertTitle>
          <AlertDescription>
            The stream checker service automatically starts when the application launches with a pipeline other than "Disabled" selected.
          </AlertDescription>
        </Alert>
      )}

      {/* Global Check Schedule */}
      {showScheduleSettings && (
        <Card>
          <CardHeader>
            <CardTitle>Global Check Schedule</CardTitle>
            <CardDescription>
              Configure when the scheduled Global Action runs. This performs a complete cycle: Updates all M3U playlists, matches all streams, and checks ALL channels (bypassing the 2-hour immunity).
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="cron_expression">Cron Expression</Label>
              <Input
                id="cron_expression"
                type="text"
                value={streamCheckerConfig.global_check_schedule?.cron_expression ?? '0 3 * * *'}
                onChange={(e) => handleStreamCheckerConfigChange('global_check_schedule.cron_expression', e.target.value)}
                placeholder="0 3 * * *"
              />
              <p className="text-sm text-muted-foreground">Enter a cron expression (e.g., '0 3 * * *' for daily at 3:00 AM, '0 3 1 * *' for monthly on the 1st at 3:00 AM)</p>
            </div>
            
            <Alert>
              <AlertCircle className="h-4 w-4" />
              <AlertTitle>Cron Expression Format</AlertTitle>
              <AlertDescription>
                <p className="font-semibold mb-2">Format: minute hour day month weekday</p>
                <p className="mb-1">Common examples:</p>
                <ul className="list-disc list-inside space-y-1">
                  <li><code className="text-sm">0 3 * * *</code> - Every day at 3:00 AM</li>
                  <li><code className="text-sm">30 2 * * *</code> - Every day at 2:30 AM</li>
                  <li><code className="text-sm">0 3 1 * *</code> - Monthly on the 1st at 3:00 AM</li>
                  <li><code className="text-sm">0 0 * * 0</code> - Every Sunday at midnight</li>
                  <li><code className="text-sm">0 */6 * * *</code> - Every 6 hours</li>
                </ul>
              </AlertDescription>
            </Alert>
          </CardContent>
        </Card>
      )}

      {/* Queue Settings */}
      {pipelineMode && pipelineMode !== 'disabled' && (
        <Card>
          <CardHeader>
            <CardTitle>Queue Settings</CardTitle>
            <CardDescription>
              Configure channel checking queue
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="max_queue_size">Maximum Queue Size</Label>
              <Input
                id="max_queue_size"
                type="number"
                min="10"
                max="10000"
                value={streamCheckerConfig.queue?.max_size ?? 1000}
                onChange={(e) => handleStreamCheckerConfigChange('queue.max_size', parseInt(e.target.value))}
              />
              <p className="text-sm text-muted-foreground">Maximum number of channels in the checking queue</p>
            </div>

            <div className="space-y-2">
              <Label htmlFor="max_channels_per_run">Max Channels Per Run</Label>
              <Input
                id="max_channels_per_run"
                type="number"
                min="1"
                max="500"
                value={streamCheckerConfig.queue?.max_channels_per_run ?? 50}
                onChange={(e) => handleStreamCheckerConfigChange('queue.max_channels_per_run', parseInt(e.target.value))}
              />
              <p className="text-sm text-muted-foreground">Maximum channels to check in a single run</p>
            </div>

            <div className="flex items-center space-x-2 pt-4">
              <Switch
                id="check_on_update"
                checked={streamCheckerConfig.queue?.check_on_update === true || streamCheckerConfig.queue?.check_on_update === undefined}
                onCheckedChange={(checked) => handleStreamCheckerConfigChange('queue.check_on_update', checked)}
              />
              <Label htmlFor="check_on_update">Check Channels on M3U Update</Label>
            </div>
            <p className="text-sm text-muted-foreground">Automatically queue channels for checking when they receive M3U playlist updates.</p>
          </CardContent>
        </Card>
      )}

      {/* Save Button */}
      <div className="flex justify-end">
        <Button onClick={handleSave} disabled={saving} size="lg">
          {saving && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
          Save Settings
        </Button>
      </div>
    </div>
  )
}
