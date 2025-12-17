import { useState, useEffect } from 'react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card.jsx'
import { Button } from '@/components/ui/button.jsx'
import { Label } from '@/components/ui/label.jsx'
import { Switch } from '@/components/ui/switch.jsx'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select.jsx'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert.jsx'
import { Loader2, AlertCircle, Trash2, Plus } from 'lucide-react'
import { useToast } from '@/hooks/use-toast.js'
import { profileAPI } from '@/services/api.js'

export default function ProfileManagement() {
  const [profileConfig, setProfileConfig] = useState(null)
  const [profiles, setProfiles] = useState([])
  const [snapshots, setSnapshots] = useState({})
  const [initialLoading, setInitialLoading] = useState(true)
  const [loadingProfiles, setLoadingProfiles] = useState(false)
  const [disablingEmptyChannels, setDisablingEmptyChannels] = useState(false)
  const [refreshingProfiles, setRefreshingProfiles] = useState(false)
  const [diagnosing, setDiagnosing] = useState(false)
  const [diagnosticInfo, setDiagnosticInfo] = useState(null)
  const [saving, setSaving] = useState(false)
  
  const { toast } = useToast()

  useEffect(() => {
    loadProfileData()
  }, [])

  const loadProfileData = async () => {
    try {
      setInitialLoading(true)
      const [profileConfigResponse, profilesResponse, snapshotsResponse] = await Promise.all([
        profileAPI.getConfig().catch(() => ({ data: null })),
        profileAPI.getProfiles().catch(() => ({ data: [] })),
        profileAPI.getAllSnapshots().catch(() => ({ data: {} }))
      ])
      setProfileConfig(profileConfigResponse.data)
      setProfiles(profilesResponse.data || [])
      setSnapshots(snapshotsResponse.data || {})
    } catch (err) {
      console.error('Failed to load profile data:', err)
      toast({
        title: "Error",
        description: "Failed to load profile configuration",
        variant: "destructive"
      })
    } finally {
      setInitialLoading(false)
    }
  }

  const handleSave = async () => {
    try {
      setSaving(true)
      if (profileConfig) {
        await profileAPI.updateConfig(profileConfig)
      }
      toast({
        title: "Success",
        description: "Profile configuration saved successfully",
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

  const handleProfileConfigChange = (field, value) => {
    // Guard against prototype pollution
    const dangerousKeys = ['__proto__', 'constructor', 'prototype']
    
    if (field.includes('.')) {
      const parts = field.split('.')
      setProfileConfig(prev => {
        const newConfig = { ...prev }
        let current = newConfig
        for (let i = 0; i < parts.length - 1; i++) {
          const key = parts[i]
          // Check each intermediate key for prototype pollution
          if (dangerousKeys.includes(key)) {
            console.warn('Attempted to set dangerous property:', key)
            return prev  // Return unchanged config
          }
          if (!current[key] || typeof current[key] !== 'object') {
            current[key] = {}
          }
          current = current[key]
        }
        // Check the final key
        const finalKey = parts[parts.length - 1]
        if (dangerousKeys.includes(finalKey)) {
          console.warn('Attempted to set dangerous property:', finalKey)
          return prev  // Return unchanged config
        }
        current[finalKey] = value
        return newConfig
      })
    } else {
      if (dangerousKeys.includes(field)) {
        console.warn('Attempted to set dangerous property:', field)
        return
      }
      
      setProfileConfig(prev => ({
        ...prev,
        [field]: value
      }))
    }
  }

  const handleCreateSnapshot = async (profileId) => {
    try {
      setLoadingProfiles(true)
      const response = await profileAPI.createSnapshot(profileId)
      setSnapshots(prev => ({
        ...prev,
        [profileId]: response.data.snapshot
      }))
      toast({
        title: "Success",
        description: "Profile snapshot created successfully",
      })
    } catch (err) {
      toast({
        title: "Error",
        description: err.response?.data?.error || "Failed to create snapshot",
        variant: "destructive"
      })
    } finally {
      setLoadingProfiles(false)
    }
  }

  const handleDeleteSnapshot = async (profileId) => {
    try {
      setLoadingProfiles(true)
      await profileAPI.deleteSnapshot(profileId)
      setSnapshots(prev => {
        const newSnapshots = { ...prev }
        delete newSnapshots[profileId]
        return newSnapshots
      })
      toast({
        title: "Success",
        description: "Snapshot deleted successfully",
      })
    } catch (err) {
      toast({
        title: "Error",
        description: err.response?.data?.error || "Failed to delete snapshot",
        variant: "destructive"
      })
    } finally {
      setLoadingProfiles(false)
    }
  }

  const handleDisableEmptyChannels = async (profileId) => {
    try {
      setDisablingEmptyChannels(true)
      const response = await profileAPI.disableEmptyChannels(profileId)
      toast({
        title: "Success",
        description: response.data.message || "Empty channels disabled successfully",
      })
    } catch (err) {
      toast({
        title: "Error",
        description: err.response?.data?.error || "Failed to disable empty channels",
        variant: "destructive"
      })
    } finally {
      setDisablingEmptyChannels(false)
    }
  }

  const handleRefreshProfiles = async () => {
    try {
      setRefreshingProfiles(true)
      const response = await profileAPI.refreshProfiles()
      
      if (response.data.success) {
        setProfiles(response.data.profiles || [])
        toast({
          title: "Success",
          description: response.data.message || `Refreshed ${response.data.profile_count} profiles`,
        })
      } else {
        throw new Error(response.data.message || "Failed to refresh profiles")
      }
    } catch (err) {
      toast({
        title: "Error",
        description: err.response?.data?.error || err.message || "Failed to refresh profiles",
        variant: "destructive"
      })
    } finally {
      setRefreshingProfiles(false)
    }
  }

  const handleDiagnoseProfiles = async () => {
    try {
      setDiagnosing(true)
      const response = await profileAPI.diagnoseProfiles()
      setDiagnosticInfo(response.data)
      
      if (response.data.cache_profile_count === 0) {
        toast({
          title: "No Profiles Found",
          description: "Check the diagnostic details below for possible causes",
          variant: "destructive"
        })
      } else {
        toast({
          title: "Diagnostic Complete",
          description: `Found ${response.data.cache_profile_count} profiles in cache`,
        })
      }
    } catch (err) {
      toast({
        title: "Error",
        description: err.response?.data?.error || "Failed to run diagnostics",
        variant: "destructive"
      })
      setDiagnosticInfo(null)
    } finally {
      setDiagnosing(false)
    }
  }

  // Show loading spinner during initial load
  if (initialLoading) {
    return (
      <div className="flex items-center justify-center min-h-[400px]">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Channel Profile Management</CardTitle>
          <CardDescription>
            Manage Dispatcharr channel profiles for selective channel control and dead stream handling
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-6">
          {/* Profile Diagnostics Section - Only show after initial load */}
          {profiles.length === 0 && !initialLoading && (
            <Alert variant="destructive">
              <AlertCircle className="h-4 w-4" aria-hidden="true" />
              <AlertTitle>No Channel Profiles Found</AlertTitle>
              <AlertDescription className="space-y-3">
                <p>Streamflow is not detecting any channel profiles from Dispatcharr.</p>
                <div className="flex flex-wrap gap-2 mt-2">
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={handleRefreshProfiles}
                    disabled={refreshingProfiles}
                    aria-label="Refresh channel profiles from Dispatcharr"
                  >
                    {refreshingProfiles && <Loader2 className="mr-2 h-4 w-4 animate-spin" aria-label="Refreshing profiles" />}
                    Refresh Profiles
                  </Button>
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={handleDiagnoseProfiles}
                    disabled={diagnosing}
                    aria-label="Run diagnostic tests for profile fetching"
                  >
                    {diagnosing && <Loader2 className="mr-2 h-4 w-4 animate-spin" aria-label="Running diagnostics" />}
                    Run Diagnostics
                  </Button>
                </div>
              </AlertDescription>
            </Alert>
          )}

          {/* Diagnostic Results */}
          {diagnosticInfo && (
            <Alert>
              <AlertCircle className="h-4 w-4" aria-hidden="true" />
              <AlertTitle>Profile Diagnostic Results</AlertTitle>
              <AlertDescription className="space-y-2">
                <div className="grid grid-cols-2 gap-2 text-sm mt-2">
                  <div>UDI Initialized:</div>
                  <div className="font-medium">{diagnosticInfo.udi_initialized ? 'Yes' : 'No'}</div>
                  
                  <div>Dispatcharr Configured:</div>
                  <div className="font-medium">{diagnosticInfo.dispatcharr_configured ? 'Yes' : 'No'}</div>
                  
                  <div>Profiles in Cache:</div>
                  <div className="font-medium">{diagnosticInfo.cache_profile_count}</div>
                  
                  <div>Profiles in Storage:</div>
                  <div className="font-medium">{diagnosticInfo.storage_profile_count}</div>
                  
                  {diagnosticInfo.last_refresh_time && (
                    <>
                      <div>Last Refresh:</div>
                      <div className="font-medium text-xs">{new Date(diagnosticInfo.last_refresh_time).toLocaleString()}</div>
                    </>
                  )}
                </div>

                {diagnosticInfo.possible_causes && diagnosticInfo.possible_causes.length > 0 && (
                  <div className="mt-3">
                    <p className="font-medium text-sm mb-1">Possible Causes:</p>
                    <ul className="list-disc list-inside text-sm space-y-1">
                      {diagnosticInfo.possible_causes.map((cause, idx) => (
                        <li key={idx}>{cause}</li>
                      ))}
                    </ul>
                  </div>
                )}

                {diagnosticInfo.recommended_actions && diagnosticInfo.recommended_actions.length > 0 && (
                  <div className="mt-3">
                    <p className="font-medium text-sm mb-1">Recommended Actions:</p>
                    <ol className="list-decimal list-inside text-sm space-y-1">
                      {diagnosticInfo.recommended_actions.map((action, idx) => (
                        <li key={idx}>{action}</li>
                      ))}
                    </ol>
                  </div>
                )}

                <Button
                  size="sm"
                  variant="ghost"
                  onClick={() => setDiagnosticInfo(null)}
                  className="mt-2"
                >
                  Close
                </Button>
              </AlertDescription>
            </Alert>
          )}

          {/* Profile Selection */}
          <div className="space-y-4">
            <div className="flex items-center space-x-2">
              <Switch
                id="use_profile"
                checked={profileConfig?.use_profile || false}
                onCheckedChange={(checked) => handleProfileConfigChange('use_profile', checked)}
              />
              <Label htmlFor="use_profile" className="text-base font-medium">
                Use Specific Profile
              </Label>
            </div>
            <p className="text-sm text-muted-foreground">
              Enable to use a specific Dispatcharr channel profile instead of the general channel list.
            </p>

            {profileConfig?.use_profile && (
              <div className="ml-6 space-y-4">
                <div className="space-y-2">
                  <Label htmlFor="selected_profile">Selected Profile</Label>
                  <Select
                    value={profileConfig?.selected_profile_id?.toString() || ''}
                    onValueChange={(value) => {
                      const profileId = parseInt(value)
                      const profile = profiles.find(p => p.id === profileId)
                      handleProfileConfigChange('selected_profile_id', profileId)
                      handleProfileConfigChange('selected_profile_name', profile?.name || '')
                    }}
                  >
                    <SelectTrigger>
                      <SelectValue placeholder="Select a profile" />
                    </SelectTrigger>
                    <SelectContent>
                      {profiles.map((profile) => (
                        <SelectItem key={profile.id} value={profile.id.toString()}>
                          {profile.name}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
              </div>
            )}
          </div>

          {/* Snapshot Management - Always visible for Empty Channel Management */}
          <div className="border-t pt-6">
            <h3 className="text-lg font-medium mb-4">Snapshot Management</h3>
            <p className="text-sm text-muted-foreground mb-4">
              Snapshots track which channels should be enabled in a profile. 
              Useful for re-enabling channels after they've been disabled by Empty Channel Management.
            </p>
            
            {profiles.length === 0 ? (
              <Alert>
                <AlertCircle className="h-4 w-4" />
                <AlertDescription>
                  No profiles available. Please refresh the profiles list.
                </AlertDescription>
              </Alert>
            ) : (
              <div className="space-y-4">
                <div className="space-y-2">
                  <Label htmlFor="snapshot_profile">Select Profile for Snapshot</Label>
                  <Select
                    value={profileConfig?.selected_profile_id?.toString() || ''}
                    onValueChange={(value) => {
                      const profileId = parseInt(value)
                      const profile = profiles.find(p => p.id === profileId)
                      handleProfileConfigChange('selected_profile_id', profileId)
                      handleProfileConfigChange('selected_profile_name', profile?.name || '')
                    }}
                  >
                    <SelectTrigger>
                      <SelectValue placeholder="Select a profile" />
                    </SelectTrigger>
                    <SelectContent>
                      {profiles.map((profile) => (
                        <SelectItem key={profile.id} value={profile.id.toString()}>
                          {profile.name}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>

                {profileConfig?.selected_profile_id && (
                  <div className="space-y-2">
                    {snapshots[profileConfig.selected_profile_id] ? (
                      <>
                        <Alert>
                          <AlertCircle className="h-4 w-4" />
                          <AlertTitle>Snapshot Exists</AlertTitle>
                          <AlertDescription>
                            Created: {new Date(snapshots[profileConfig.selected_profile_id].created_at).toLocaleString()}
                            <br />
                            Channels: {snapshots[profileConfig.selected_profile_id].channel_count}
                          </AlertDescription>
                        </Alert>
                        <Button
                          variant="destructive"
                          size="sm"
                          onClick={() => handleDeleteSnapshot(profileConfig.selected_profile_id)}
                          disabled={loadingProfiles}
                        >
                          {loadingProfiles ? (
                            <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                          ) : (
                            <Trash2 className="mr-2 h-4 w-4" />
                          )}
                          Delete Snapshot
                        </Button>
                      </>
                    ) : (
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => handleCreateSnapshot(profileConfig.selected_profile_id)}
                        disabled={loadingProfiles}
                      >
                        {loadingProfiles ? (
                          <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                        ) : (
                          <Plus className="mr-2 h-4 w-4" />
                        )}
                        Create Snapshot
                      </Button>
                    )}
                  </div>
                )}
              </div>
            )}
          </div>

          <div className="border-t pt-6">
            <h3 className="text-lg font-medium mb-4">Empty Channel Management</h3>
            
            <div className="space-y-4">
              <div className="flex items-center space-x-2">
                <Switch
                  id="dead_streams_enabled"
                  checked={profileConfig?.dead_streams?.enabled || false}
                  onCheckedChange={(checked) => handleProfileConfigChange('dead_streams.enabled', checked)}
                />
                <Label htmlFor="dead_streams_enabled" className="text-base font-medium">
                  Enable Empty Channel Management
                </Label>
              </div>
              <p className="text-sm text-muted-foreground">
                Automatically disable channels with no working streams in a target profile
              </p>

              {profileConfig?.dead_streams?.enabled && (
                <div className="space-y-4 ml-6">
                  <div className="space-y-2">
                    <Label htmlFor="target_profile">Target Profile for Disabling</Label>
                    <Select
                      value={profileConfig?.dead_streams?.target_profile_id?.toString() || ''}
                      onValueChange={(value) => {
                        const profileId = parseInt(value)
                        const profile = profiles.find(p => p.id === profileId)
                        handleProfileConfigChange('dead_streams.target_profile_id', profileId)
                        handleProfileConfigChange('dead_streams.target_profile_name', profile?.name || '')
                      }}
                    >
                      <SelectTrigger>
                        <SelectValue placeholder="Select target profile" />
                      </SelectTrigger>
                      <SelectContent>
                        {profiles.map((profile) => (
                          <SelectItem key={profile.id} value={profile.id.toString()}>
                            {profile.name}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                    <p className="text-sm text-muted-foreground">
                      Profile where empty channels will be disabled. Can be different from the selected profile.
                    </p>
                  </div>

                  <div className="flex items-center space-x-2">
                    <Switch
                      id="use_snapshot"
                      checked={profileConfig?.dead_streams?.use_snapshot || false}
                      onCheckedChange={(checked) => handleProfileConfigChange('dead_streams.use_snapshot', checked)}
                    />
                    <Label htmlFor="use_snapshot">
                      Use Snapshot for Re-enabling
                    </Label>
                  </div>
                  <p className="text-sm text-muted-foreground ml-6">
                    When enabled, only channels in the snapshot will be considered for disabling.
                    A snapshot must be created for the target profile.
                  </p>

                  {profileConfig?.dead_streams?.use_snapshot && profileConfig?.dead_streams?.target_profile_id && !snapshots[profileConfig.dead_streams.target_profile_id] && (
                    <Alert variant="warning" className="ml-6">
                      <AlertCircle className="h-4 w-4" />
                      <AlertTitle>Snapshot Required</AlertTitle>
                      <AlertDescription>
                        Snapshot mode is enabled but no snapshot exists for the target profile.
                        {profileConfig.dead_streams.target_profile_id === profileConfig.selected_profile_id ? (
                          <span> Use the snapshot management section above to create one.</span>
                        ) : (
                          <span> Select the target profile as your active profile first, then create a snapshot.</span>
                        )}
                      </AlertDescription>
                    </Alert>
                  )}

                  {profileConfig?.dead_streams?.target_profile_id && (
                    <div className="mt-4">
                      <Button
                        variant="outline"
                        onClick={() => handleDisableEmptyChannels(profileConfig.dead_streams.target_profile_id)}
                        disabled={disablingEmptyChannels}
                      >
                        {disablingEmptyChannels && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                        Disable Empty Channels Now
                      </Button>
                      <p className="text-sm text-muted-foreground mt-2">
                        Manually trigger disabling of channels with no working streams in the target profile
                      </p>
                    </div>
                  )}
                </div>
              )}
            </div>
          </div>

          <Alert>
            <AlertCircle className="h-4 w-4" />
            <AlertTitle>How Profile Management Works</AlertTitle>
            <AlertDescription className="space-y-2 mt-2">
              <p>
                <strong>Profile Selection:</strong> Choose a specific Dispatcharr channel profile to use instead of the general channel list.
                When enabled, only channels that are enabled in the selected profile will be used for stream checking and management.
              </p>
              <p>
                <strong>Snapshot Management:</strong> Create snapshots to track which channels should be enabled in a profile.
                This is valid for both general operation when using a specific profile, and for empty channel management.
              </p>
              <p>
                <strong>Empty Channel Management:</strong> Automatically disable channels with no working streams in a target profile, 
                keeping your channel list clean and organized. The target profile can be the same as or different from your selected profile.
              </p>
            </AlertDescription>
          </Alert>
        </CardContent>
      </Card>

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
