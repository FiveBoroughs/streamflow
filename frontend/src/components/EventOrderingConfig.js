import React, { useState, useEffect } from 'react';
import {
  Box,
  Typography,
  Card,
  CardContent,
  TextField,
  Button,
  Alert,
  CircularProgress,
  Grid,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Chip,
  IconButton,
  Paper,
  Tooltip
} from '@mui/material';
import {
  ContentCopy as CopyIcon,
  PlayArrow as TestIcon,
  Save as SaveIcon,
  Refresh as RefreshIcon
} from '@mui/icons-material';
import { useNavigate } from 'react-router-dom';
import { channelsAPI, eventOrderingAPI, api } from '../services/api';

function EventOrderingConfig() {
  const navigate = useNavigate();

  // State
  const [channels, setChannels] = useState([]);
  const [selectedChannelId, setSelectedChannelId] = useState('');
  const [streams, setStreams] = useState([]);
  const [selectedStreamName, setSelectedStreamName] = useState('');
  const [loading, setLoading] = useState(true);
  const [loadingStreams, setLoadingStreams] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');

  // Regex builder state
  const [words, setWords] = useState([]);
  const [wordData, setWordData] = useState({});
  const [selectedWords, setSelectedWords] = useState(new Set());

  // Categories for time components
  const categories = [
    { id: 'league', label: 'League', color: '#f97316', tooltip: 'Event type (UFC, NBA, etc.)' },
    { id: 'order', label: 'Order', color: '#a855f7', tooltip: 'Event number for tiebreaking when times match (lower first)' },
    { id: 'year', label: 'Year', color: '#3b82f6' },
    { id: 'month', label: 'Month', color: '#8b5cf6' },
    { id: 'day', label: 'Day', color: '#06b6d4' },
    { id: 'hour', label: 'Hour', color: '#10b981' },
    { id: 'minute', label: 'Minute', color: '#f59e0b' },
    { id: 'second', label: 'Second', color: '#ef4444' },
    { id: 'ampm', label: 'AM/PM', color: '#ec4899' },
    { id: 'ignore', label: 'Ignore', color: '#6b7280' }
  ];

  // Generated pattern
  const [generatedPattern, setGeneratedPattern] = useState('');
  const [testResults, setTestResults] = useState([]);
  const [orderingPreview, setOrderingPreview] = useState([]);

  // Config state
  const [config, setConfig] = useState(null);

  useEffect(() => {
    loadInitialData();
  }, []);

  const loadInitialData = async () => {
    try {
      setLoading(true);
      const [channelsResponse, configResponse] = await Promise.all([
        channelsAPI.getChannels(),
        eventOrderingAPI.getConfig()
      ]);
      setChannels(channelsResponse.data || []);
      setConfig(configResponse.data || { enabled: false, frequency: 300, channels: {} });
    } catch (err) {
      console.error('Failed to load data:', err);
      setError('Failed to load configuration');
    } finally {
      setLoading(false);
    }
  };

  const handleChannelChange = async (channelId) => {
    setSelectedChannelId(channelId);
    setSelectedStreamName('');
    setWords([]);
    setWordData({});
    setGeneratedPattern('');
    setTestResults([]);
    setOrderingPreview([]);

    if (!channelId) {
      setStreams([]);
      return;
    }

    try {
      setLoadingStreams(true);
      const response = await api.get(`/channels/${channelId}/streams`);
      const streamData = response.data || [];
      setStreams(streamData);

      // Auto-select first stream if available
      if (streamData.length > 0) {
        const firstStreamName = streamData[0].name;
        setSelectedStreamName(firstStreamName);
        parseSample(firstStreamName);
      } else {
        setError('No streams found in this channel');
      }

      // Load existing pattern if configured (after parseSample so it doesn't get cleared)
      if (config?.channels?.[channelId]) {
        const existingPattern = config.channels[channelId].pattern || '';
        setGeneratedPattern(existingPattern);
        // Test the existing pattern against streams
        if (existingPattern && streamData.length > 0) {
          testPattern(existingPattern, streamData);
        }
      }
    } catch (err) {
      console.error('Failed to load streams:', err);
      setError('Failed to load streams for channel');
      setStreams([]);
    } finally {
      setLoadingStreams(false);
    }
  };

  const handleStreamSelect = (streamName) => {
    setSelectedStreamName(streamName);
    parseSample(streamName);
  };

  const parseSample = (sample) => {
    if (!sample) {
      setWords([]);
      setWordData({});
      return;
    }

    // Split on whitespace and common delimiters, preserving delimiters
    const tokens = sample.split(/(\s+|[|:@()#\-/])/g)
      .filter(t => t && t.length > 0);

    const newWords = [];
    const newWordData = {};

    let wordIndex = 0;
    tokens.forEach((token) => {
      // Skip pure whitespace tokens but mark their position
      if (/^\s+$/.test(token)) {
        // Add a space marker
        const id = `word-${wordIndex}`;
        newWords.push({ id, text: ' ', isSpace: true });
        newWordData[id] = { text: ' ', category: null, index: wordIndex, isSpace: true };
        wordIndex++;
      } else {
        const id = `word-${wordIndex}`;
        newWords.push({ id, text: token.trim() });
        newWordData[id] = { text: token.trim(), category: null, index: wordIndex };
        wordIndex++;
      }
    });

    setWords(newWords);
    setWordData(newWordData);
    setSelectedWords(new Set());
    setGeneratedPattern('');
    setTestResults([]);
  };

  // Drag and drop handlers
  const handleDragStart = (e, wordId) => {
    const wordsToMove = selectedWords.has(wordId)
      ? Array.from(selectedWords)
      : [wordId];

    e.dataTransfer.setData('text/plain', JSON.stringify(wordsToMove));
    e.dataTransfer.effectAllowed = 'move';
  };

  const handleDragOver = (e) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = 'move';
  };

  const handleDrop = (e, category) => {
    e.preventDefault();

    try {
      const wordIds = JSON.parse(e.dataTransfer.getData('text/plain'));

      setWordData(prev => {
        const updated = { ...prev };
        wordIds.forEach(id => {
          if (updated[id]) {
            updated[id] = { ...updated[id], category };
          }
        });
        return updated;
      });

      setSelectedWords(new Set());
    } catch (err) {
      console.error('Drop error:', err);
    }
  };

  const handleWordClick = (e, wordId) => {
    if (e.shiftKey || e.ctrlKey || e.metaKey) {
      setSelectedWords(prev => {
        const updated = new Set(prev);
        if (updated.has(wordId)) {
          updated.delete(wordId);
        } else {
          updated.add(wordId);
        }
        return updated;
      });
    } else {
      setSelectedWords(new Set([wordId]));
    }
  };

  const removeFromCategory = (wordId) => {
    setWordData(prev => ({
      ...prev,
      [wordId]: { ...prev[wordId], category: null }
    }));
  };

  const generateRegex = () => {
    // Build pattern based on categorized words
    const categorizedWords = Object.entries(wordData)
      .filter(([_, data]) => data.category && data.category !== 'ignore')
      .sort((a, b) => a[1].index - b[1].index);

    if (categorizedWords.length === 0) {
      setError('Please categorize at least one word');
      return;
    }

    // Get all words sorted by index
    const allWords = Object.entries(wordData).sort((a, b) => a[1].index - b[1].index);

    let pattern = '';
    let lastCategorizedIndex = -1;

    allWords.forEach(([id, data]) => {
      if (data.category && data.category !== 'ignore') {
        // Check what's between last categorized word and this one
        if (lastCategorizedIndex >= 0 && data.index > lastCategorizedIndex + 1) {
          // Get tokens between
          const betweenTokens = allWords
            .filter(([_, d]) => d.index > lastCategorizedIndex && d.index < data.index)
            .map(([_, d]) => d.text);

          // If all tokens are single-char delimiters, include them literally
          const allDelimiters = betweenTokens.every(t => /^[-:\/\s]$/.test(t));
          if (allDelimiters && betweenTokens.length > 0) {
            pattern += betweenTokens.map(t => t === ' ' ? '\\s+' : escapeRegex(t)).join('');
          } else {
            pattern += '.*?';
          }
        }

        // Add named capture group
        pattern += `(?<${data.category}>${buildCapturePattern(data.text, data.category)})`;
        lastCategorizedIndex = data.index;
      }
    });

    setGeneratedPattern(pattern);
    testPattern(pattern);
  };

  const buildCapturePattern = (text, category) => {
    // Build appropriate regex pattern based on category
    switch (category) {
      case 'league':
        return '[A-Za-z]+';
      case 'order':
        return '\\d+';
      case 'year':
        return '\\d{4}';
      case 'month':
        // Could be numeric (01-12) or text (Jan, January)
        if (/^\d+$/.test(text)) {
          return '\\d{1,2}';
        }
        return '[A-Za-z]+';
      case 'day':
        return '\\d{1,2}';
      case 'hour':
        return '\\d{1,2}';
      case 'minute':
        return '\\d{2}';
      case 'second':
        return '\\d{2}';
      case 'ampm':
        return '[AaPp][Mm]';
      default:
        return escapeRegex(text);
    }
  };

  const escapeRegex = (str) => {
    return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  };

  const testPattern = (pattern, streamList = null) => {
    const streamsToTest = streamList || streams;
    if (!pattern || streamsToTest.length === 0) {
      setTestResults([]);
      setOrderingPreview([]);
      return;
    }

    try {
      const regex = new RegExp(pattern, 'i');
      const results = streamsToTest.slice(0, 100).map(stream => {
        const match = stream.name.match(regex);
        return {
          name: stream.name,
          matched: !!match,
          groups: match?.groups || null
        };
      });
      setTestResults(results);

      // Generate ordering preview
      const now = new Date();
      const bufferHours = 2;

      const parsedStreams = streamsToTest.map((stream, idx) => {
        const match = stream.name.match(regex);
        let eventTime = null;
        let orderNum = 999;

        if (match?.groups) {
          const g = match.groups;

          // Extract order
          if (g.order) {
            orderNum = parseInt(g.order) || 999;
          }

          // Build datetime
          const year = g.year ? parseInt(g.year) : now.getFullYear();
          let month = now.getMonth();
          if (g.month) {
            if (/^\d+$/.test(g.month)) {
              month = parseInt(g.month) - 1;
            } else {
              const months = {jan:0,feb:1,mar:2,apr:3,may:4,jun:5,jul:6,aug:7,sep:8,oct:9,nov:10,dec:11};
              month = months[g.month.toLowerCase().slice(0,3)] ?? now.getMonth();
            }
          }
          const day = g.day ? parseInt(g.day) : now.getDate();
          let hour = g.hour ? parseInt(g.hour) : 0;

          // Handle AM/PM
          if (g.ampm) {
            const ampm = g.ampm.toUpperCase();
            if (ampm === 'PM' && hour !== 12) hour += 12;
            if (ampm === 'AM' && hour === 12) hour = 0;
          }

          const minute = g.minute ? parseInt(g.minute) : 0;
          const second = g.second ? parseInt(g.second) : 0;

          eventTime = new Date(year, month, day, hour, minute, second);
        }

        return {
          name: stream.name,
          eventTime,
          orderNum,
          originalIndex: idx
        };
      });

      // Separate into upcoming and past
      const upcoming = [];
      const past = [];

      parsedStreams.forEach(s => {
        if (s.eventTime) {
          const hoursDiff = (now - s.eventTime) / (1000 * 60 * 60);
          if (hoursDiff < bufferHours) {
            upcoming.push(s);
          } else {
            past.push(s);
          }
        } else {
          past.push(s);
        }
      });

      // Sort: upcoming by time ASC then order, past by time DESC then order
      upcoming.sort((a, b) => {
        if (a.eventTime && b.eventTime) {
          const timeDiff = a.eventTime - b.eventTime;
          if (timeDiff !== 0) return timeDiff;
        }
        return a.orderNum - b.orderNum;
      });

      past.sort((a, b) => {
        if (a.eventTime && b.eventTime) {
          const timeDiff = b.eventTime - a.eventTime;
          if (timeDiff !== 0) return timeDiff;
        }
        return a.orderNum - b.orderNum;
      });

      const ordered = [...upcoming, ...past].map((s, newIdx) => ({
        ...s,
        newIndex: newIdx + 1,
        isUpcoming: upcoming.includes(s)
      }));

      setOrderingPreview(ordered);
    } catch (err) {
      setError(`Invalid regex pattern: ${err.message}`);
      setTestResults([]);
      setOrderingPreview([]);
    }
  };

  const handleSave = async () => {
    if (!selectedChannelId || !generatedPattern) {
      setError('Please select a channel and generate a pattern');
      return;
    }

    try {
      setSaving(true);

      const channel = channels.find(c => c.id === parseInt(selectedChannelId));
      const updatedConfig = {
        ...config,
        channels: {
          ...config.channels,
          [selectedChannelId]: {
            pattern: generatedPattern,
            name: channel?.name || `Channel ${selectedChannelId}`
          }
        }
      };

      await eventOrderingAPI.updateConfig(updatedConfig);
      setConfig(updatedConfig);
      setSuccess(`Pattern saved for ${channel?.name || 'channel'}`);
    } catch (err) {
      setError('Failed to save configuration');
    } finally {
      setSaving(false);
    }
  };

  const handleDeleteChannel = async (channelId) => {
    try {
      const updatedChannels = { ...config.channels };
      delete updatedChannels[channelId];

      const updatedConfig = {
        ...config,
        channels: updatedChannels
      };

      await eventOrderingAPI.updateConfig(updatedConfig);
      setConfig(updatedConfig);
      setSuccess('Channel configuration removed');
    } catch (err) {
      setError('Failed to remove channel configuration');
    }
  };

  const copyToClipboard = (text) => {
    navigator.clipboard.writeText(text);
    setSuccess('Copied to clipboard');
  };

  if (loading) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" height="400px">
        <CircularProgress />
      </Box>
    );
  }

  // Words available (not yet categorized, excluding spaces)
  const availableWords = words.filter(w => !wordData[w.id]?.category && !wordData[w.id]?.isSpace);

  // Words in each category
  const getCategoryWords = (categoryId) => {
    return words.filter(w => wordData[w.id]?.category === categoryId);
  };

  return (
    <Box>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
        <Typography variant="h4">
          Event Time Ordering Configuration
        </Typography>
        <Button variant="outlined" onClick={() => navigate('/settings')}>
          Back to Settings
        </Button>
      </Box>

      {error && (
        <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError('')}>
          {error}
        </Alert>
      )}

      {success && (
        <Alert severity="success" sx={{ mb: 2 }} onClose={() => setSuccess('')}>
          {success}
        </Alert>
      )}

      <Grid container spacing={2}>
        {/* Configured Channels */}
        {config?.channels && Object.keys(config.channels).length > 0 && (
          <Grid item xs={12}>
            <Card>
              <CardContent>
                <Typography variant="h6" gutterBottom>
                  Configured Channels
                </Typography>
                <Box display="flex" flexWrap="wrap" gap={1}>
                  {Object.entries(config.channels).map(([channelId, channelConfig]) => (
                    <Chip
                      key={channelId}
                      label={channelConfig.name}
                      onDelete={() => handleDeleteChannel(channelId)}
                      onClick={() => handleChannelChange(channelId)}
                      color={selectedChannelId === channelId ? 'primary' : 'default'}
                      variant={selectedChannelId === channelId ? 'filled' : 'outlined'}
                    />
                  ))}
                </Box>
              </CardContent>
            </Card>
          </Grid>
        )}

        {/* Step 1: Channel Selection */}
        <Grid item xs={12} md={6}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Step 1: Select Channel
              </Typography>
              <FormControl fullWidth>
                <InputLabel>Channel</InputLabel>
                <Select
                  value={selectedChannelId}
                  onChange={(e) => handleChannelChange(e.target.value)}
                  label="Channel"
                >
                  <MenuItem value="">
                    <em>Select a channel...</em>
                  </MenuItem>
                  {channels.map((channel) => (
                    <MenuItem key={channel.id} value={channel.id}>
                      {channel.name} (ID: {channel.id})
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </CardContent>
          </Card>
        </Grid>

        {/* Step 2: Stream Selection */}
        <Grid item xs={12} md={6}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Step 2: Select Sample Stream
              </Typography>
              {loadingStreams ? (
                <Box display="flex" justifyContent="center" p={2}>
                  <CircularProgress size={24} />
                </Box>
              ) : (
                <FormControl fullWidth disabled={!selectedChannelId}>
                  <InputLabel>Stream</InputLabel>
                  <Select
                    value={selectedStreamName}
                    onChange={(e) => handleStreamSelect(e.target.value)}
                    label="Stream"
                  >
                    <MenuItem value="">
                      <em>Select a stream...</em>
                    </MenuItem>
                    {streams.map((stream, index) => (
                      <MenuItem key={index} value={stream.name}>
                        {stream.name}
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
              )}
              {streams.length === 0 && selectedChannelId && !loadingStreams && (
                <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
                  No streams found in this channel
                </Typography>
              )}
            </CardContent>
          </Card>
        </Grid>

        {/* Step 3: Token Parser and Drag-Drop Builder */}
        {words.length > 0 && (
          <Grid item xs={12}>
            <Card>
              <CardContent>
                <Typography variant="h6" gutterBottom>
                  Step 3: Categorize Time Components
                </Typography>
                <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                  Drag words to their categories. Use Shift/Ctrl+click to select multiple words.
                </Typography>

                <Alert severity="info" sx={{ mb: 2 }}>
                  <Typography variant="body2">
                    <strong>Important:</strong> Only categorize the <strong>start time</strong> components, not the end/stop time.
                    Leave categories empty if they don't apply to your stream format.
                  </Typography>
                </Alert>

                {/* Available Words */}
                <Paper
                  variant="outlined"
                  sx={{
                    p: 2,
                    mb: 2,
                    minHeight: 60,
                    backgroundColor: 'background.default'
                  }}
                >
                  <Typography variant="subtitle2" gutterBottom color="text.primary">
                    Available Words
                  </Typography>
                  <Box display="flex" flexWrap="wrap" gap={1}>
                    {availableWords.map((word) => (
                      <Chip
                        key={word.id}
                        label={word.text}
                        draggable
                        onDragStart={(e) => handleDragStart(e, word.id)}
                        onClick={(e) => handleWordClick(e, word.id)}
                        color={selectedWords.has(word.id) ? 'primary' : 'default'}
                        sx={{
                          cursor: 'grab',
                          '&:active': { cursor: 'grabbing' }
                        }}
                      />
                    ))}
                    {availableWords.length === 0 && (
                      <Typography variant="body2" color="text.primary">
                        All words categorized
                      </Typography>
                    )}
                  </Box>
                </Paper>

                {/* Drop Zones */}
                <Grid container spacing={1}>
                  {categories.map((category) => (
                    <Grid item xs={6} sm={4} md={3} key={category.id}>
                      <Paper
                        variant="outlined"
                        onDragOver={handleDragOver}
                        onDrop={(e) => handleDrop(e, category.id)}
                        sx={{
                          p: 1.5,
                          minHeight: 80,
                          backgroundColor: `${category.color}15`,
                          borderColor: category.color,
                          borderWidth: 2,
                          borderStyle: 'dashed',
                          transition: 'all 0.2s',
                          '&:hover': {
                            backgroundColor: `${category.color}25`
                          }
                        }}
                      >
                        {category.tooltip ? (
                          <Tooltip title={category.tooltip} placement="top">
                            <Typography
                              variant="subtitle2"
                              sx={{
                                color: category.color,
                                fontWeight: 'bold',
                                mb: 1,
                                cursor: 'help'
                              }}
                            >
                              {category.label}
                            </Typography>
                          </Tooltip>
                        ) : (
                          <Typography
                            variant="subtitle2"
                            sx={{
                              color: category.color,
                              fontWeight: 'bold',
                              mb: 1
                            }}
                          >
                            {category.label}
                          </Typography>
                        )}
                        <Box display="flex" flexWrap="wrap" gap={0.5}>
                          {getCategoryWords(category.id).map((word) => (
                            <Chip
                              key={word.id}
                              label={word.text}
                              size="small"
                              onDelete={() => removeFromCategory(word.id)}
                              sx={{
                                backgroundColor: category.color,
                                color: 'white',
                                '& .MuiChip-deleteIcon': {
                                  color: 'rgba(255,255,255,0.7)',
                                  '&:hover': { color: 'white' }
                                }
                              }}
                            />
                          ))}
                        </Box>
                      </Paper>
                    </Grid>
                  ))}
                </Grid>

                <Box mt={2}>
                  <Button
                    variant="contained"
                    onClick={generateRegex}
                    startIcon={<RefreshIcon />}
                  >
                    Generate Pattern
                  </Button>
                </Box>
              </CardContent>
            </Card>
          </Grid>
        )}

        {/* Step 4: Generated Pattern */}
        {generatedPattern && (
          <Grid item xs={12}>
            <Card>
              <CardContent>
                <Typography variant="h6" gutterBottom>
                  Step 4: Generated Pattern
                </Typography>
                <Paper
                  variant="outlined"
                  sx={{
                    p: 2,
                    backgroundColor: '#1a1d23',
                    fontFamily: 'monospace'
                  }}
                >
                  <Box display="flex" justifyContent="space-between" alignItems="flex-start">
                    <Typography
                      sx={{
                        color: '#10b981',
                        fontFamily: 'monospace',
                        wordBreak: 'break-all',
                        flex: 1
                      }}
                    >
                      {generatedPattern}
                    </Typography>
                    <Tooltip title="Copy to clipboard">
                      <IconButton
                        size="small"
                        onClick={() => copyToClipboard(generatedPattern)}
                        sx={{ color: '#6b7280', ml: 1 }}
                      >
                        <CopyIcon />
                      </IconButton>
                    </Tooltip>
                  </Box>
                </Paper>

                <Box mt={2} display="flex" gap={2}>
                  <Button
                    variant="contained"
                    color="success"
                    onClick={handleSave}
                    disabled={saving}
                    startIcon={saving ? <CircularProgress size={20} /> : <SaveIcon />}
                  >
                    Save Pattern
                  </Button>
                  <Button
                    variant="outlined"
                    onClick={() => testPattern(generatedPattern)}
                    startIcon={<TestIcon />}
                  >
                    Test Pattern
                  </Button>
                </Box>
              </CardContent>
            </Card>
          </Grid>
        )}

        {/* Test Results */}
        {testResults.length > 0 && (
          <Grid item xs={12}>
            <Card>
              <CardContent>
                <Typography variant="h6" gutterBottom>
                  Test Results
                </Typography>
                <Box sx={{ maxHeight: 400, overflow: 'auto' }}>
                  {testResults.map((result, index) => (
                    <Paper
                      key={index}
                      variant="outlined"
                      sx={{
                        p: 1.5,
                        mb: 1,
                        backgroundColor: result.matched ? '#10b98115' : '#ef444415',
                        borderColor: result.matched ? '#10b981' : '#ef4444'
                      }}
                    >
                      <Typography
                        variant="body2"
                        sx={{
                          fontFamily: 'monospace',
                          wordBreak: 'break-all',
                          color: result.matched ? '#10b981' : '#ef4444'
                        }}
                      >
                        {result.matched ? '✓' : '✗'} {result.name}
                      </Typography>
                      {result.groups && (
                        <Box display="flex" flexWrap="wrap" gap={0.5} mt={1}>
                          {Object.entries(result.groups).map(([key, value]) => (
                            value && (
                              <Chip
                                key={key}
                                label={`${key}: ${value}`}
                                size="small"
                                variant="outlined"
                              />
                            )
                          ))}
                        </Box>
                      )}
                    </Paper>
                  ))}
                </Box>
              </CardContent>
            </Card>
          </Grid>
        )}

        {/* Ordering Preview */}
        {orderingPreview.length > 0 && (
          <Grid item xs={12}>
            <Card>
              <CardContent>
                <Typography variant="h6" gutterBottom>
                  Ordering Preview
                </Typography>
                <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                  How streams will be reordered after applying the pattern.
                </Typography>

                {/* Header */}
                <Box
                  display="grid"
                  gridTemplateColumns="1fr 80px 80px 80px 120px"
                  gap={1}
                  sx={{
                    p: 1,
                    backgroundColor: 'action.hover',
                    borderRadius: 1,
                    mb: 1,
                    fontWeight: 'bold'
                  }}
                >
                  <Typography variant="caption" fontWeight="bold">Name</Typography>
                  <Typography variant="caption" fontWeight="bold" textAlign="center">New Order</Typography>
                  <Typography variant="caption" fontWeight="bold" textAlign="center">Change</Typography>
                  <Typography variant="caption" fontWeight="bold" textAlign="center">Old Order</Typography>
                  <Typography variant="caption" fontWeight="bold" textAlign="center">Time</Typography>
                </Box>

                <Box sx={{ maxHeight: 400, overflow: 'auto' }}>
                  {orderingPreview.map((item, index) => {
                    const oldPos = item.originalIndex + 1;
                    const newPos = item.newIndex;
                    const change = oldPos - newPos;

                    // Calculate countdown
                    let timeDisplay = '—';
                    if (item.eventTime) {
                      const now = new Date();
                      const diff = item.eventTime - now;
                      const diffMins = Math.floor(diff / 60000);
                      const diffHours = Math.floor(diffMins / 60);
                      const diffDays = Math.floor(diffHours / 24);

                      if (diff < 0) {
                        // Past
                        const pastMins = Math.abs(diffMins);
                        const pastHours = Math.floor(pastMins / 60);
                        const pastDays = Math.floor(pastHours / 24);
                        const remainingHours = pastHours % 24;
                        const remainingMins = pastMins % 60;

                        if (pastDays > 0) {
                          timeDisplay = remainingHours > 0
                            ? `${pastDays}d ${remainingHours}h ago`
                            : `${pastDays}d ago`;
                        } else if (pastHours > 0) {
                          timeDisplay = remainingMins > 0
                            ? `${pastHours}h ${remainingMins}m ago`
                            : `${pastHours}h ago`;
                        } else {
                          timeDisplay = `${pastMins}m ago`;
                        }
                      } else {
                        // Future
                        const remainingHours = diffHours % 24;
                        const remainingMins = diffMins % 60;

                        if (diffDays > 0) {
                          timeDisplay = remainingHours > 0
                            ? `in ${diffDays}d ${remainingHours}h`
                            : `in ${diffDays}d`;
                        } else if (diffHours > 0) {
                          timeDisplay = remainingMins > 0
                            ? `in ${diffHours}h ${remainingMins}m`
                            : `in ${diffHours}h`;
                        } else {
                          timeDisplay = `in ${diffMins}m`;
                        }
                      }
                    }

                    return (
                      <Box
                        key={index}
                        display="grid"
                        gridTemplateColumns="1fr 80px 80px 80px 120px"
                        gap={1}
                        sx={{
                          p: 1,
                          borderBottom: '1px solid',
                          borderColor: 'divider',
                          '&:hover': { backgroundColor: 'action.hover' }
                        }}
                      >
                        <Typography
                          variant="body2"
                          sx={{
                            fontFamily: 'monospace',
                            overflow: 'hidden',
                            textOverflow: 'ellipsis',
                            whiteSpace: 'nowrap'
                          }}
                        >
                          {item.name}
                        </Typography>
                        <Typography variant="body2" textAlign="center" fontWeight="bold">
                          {newPos}
                        </Typography>
                        <Typography
                          variant="body2"
                          textAlign="center"
                          sx={{
                            color: change > 0 ? 'success.main' : change < 0 ? 'error.main' : 'text.secondary'
                          }}
                        >
                          {change > 0 ? `↑${change}` : change < 0 ? `↓${Math.abs(change)}` : '—'}
                        </Typography>
                        <Typography variant="body2" textAlign="center" color="text.secondary">
                          {oldPos}
                        </Typography>
                        <Typography variant="body2" textAlign="center">
                          {timeDisplay}
                        </Typography>
                      </Box>
                    );
                  })}
                </Box>
              </CardContent>
            </Card>
          </Grid>
        )}

        {/* Manual Pattern Entry */}
        <Grid item xs={12}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Manual Pattern Entry
              </Typography>
              <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                Or enter a custom regex pattern directly. Use named capture groups for time components.
              </Typography>
              <TextField
                fullWidth
                label="Custom Pattern"
                value={generatedPattern}
                onChange={(e) => setGeneratedPattern(e.target.value)}
                placeholder="e.g., start:(?<year>\d{4})-(?<month>\d{2})-(?<day>\d{2}) (?<hour>\d{2}):(?<minute>\d{2})"
                helperText="Named groups: year, month, day, hour, minute, second, ampm"
                sx={{ mb: 2 }}
              />
              <Box display="flex" gap={2}>
                <Button
                  variant="outlined"
                  onClick={() => testPattern(generatedPattern)}
                  startIcon={<TestIcon />}
                  disabled={!generatedPattern}
                >
                  Test Pattern
                </Button>
                <Button
                  variant="contained"
                  color="success"
                  onClick={handleSave}
                  disabled={saving || !generatedPattern || !selectedChannelId}
                  startIcon={saving ? <CircularProgress size={20} /> : <SaveIcon />}
                >
                  Save Pattern
                </Button>
              </Box>
            </CardContent>
          </Card>
        </Grid>
      </Grid>
    </Box>
  );
}

export default EventOrderingConfig;
