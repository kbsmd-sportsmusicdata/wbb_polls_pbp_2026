import React, { useState, useEffect } from 'react';
import { CheckCircle2, Circle, ChevronDown, ChevronRight, Database, BarChart3, Zap, Target, Save, RefreshCw } from 'lucide-react';

// ==============================================================================
// WBB AP POLL - PROJECT TRACKER
// Weekly AP Poll analytics with rank movement, volatility, and conference views
// ==============================================================================

const PROJECT_NAME = "WBB AP Poll Analytics";
const STORAGE_KEY = 'wbb-polls-pbp-2026-tracker';

const ProjectTracker = () => {
  const [expandedSections, setExpandedSections] = useState({
    phase1: true,
    phase2: true,
    phase3: true,
    phase4: true
  });

  const [tasks, setTasks] = useState({
    // PHASE 1: Data Pipeline (Week 1) - 8 hrs
    'setup-repo-structure': false,
    'install-dependencies': false,
    'create-gitignore': false,
    'build-scraper': false,
    'store-raw-data': false,
    'build-polls-long': false,
    'implement-current-rank': false,
    'implement-previous-rank': false,
    'implement-rank-change': false,
    'implement-movement-category': false,
    'implement-ranked-flags': false,
    'implement-conference-grouping': false,

    // PHASE 2: Tableau Build (Week 2-3) - 12 hrs
    'tableau-snapshot-table': false,
    'tableau-rank-change-bar': false,
    'tableau-volatility-view': false,
    'tableau-conference-lens': false,
    'filter-poll-week': false,
    'filter-conference': false,
    'filter-movement-category': false,
    'filter-team-search': false,
    'design-movement-colors': false,
    'design-diverging-bars': false,
    'design-visual-hierarchy': false,
    'design-mobile-friendly': false,
    'annotation-tooltips': false,
    'annotation-captions': false,
    'annotation-movement-context': false,
    'annotation-volatility-insights': false,

    // PHASE 3: Automation & QA (Week 4) - 3 hrs
    'qa-scraper-errors': false,
    'qa-append-logic': false,
    'qa-unranked-handling': false,
    'qa-filter-integrity': false,
    'qa-zero-line': false,
    'qa-cross-view': false,

    // PHASE 4: Publishing (Week 4) - 2 hrs
    'publish-tableau-public': false,
    'update-dashboard-description': false,
    'update-readme': false,
    'portfolio-integration': false
  });

  const [saveStatus, setSaveStatus] = useState('');
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    const loadSavedState = async () => {
      try {
        const result = await window.storage.get(STORAGE_KEY);
        if (result && result.value) {
          const savedData = JSON.parse(result.value);
          if (savedData.tasks) {
            setTasks(prev => ({ ...prev, ...savedData.tasks }));
          }
          if (savedData.expandedSections) {
            setExpandedSections(prev => ({ ...prev, ...savedData.expandedSections }));
          }
          setSaveStatus('✓ Loaded saved progress');
        } else {
          setSaveStatus('Starting fresh');
        }
      } catch (error) {
        console.log('No saved state found, using defaults');
        setSaveStatus('Starting fresh');
      }
      setIsLoading(false);
    };

    loadSavedState();
  }, []);

  useEffect(() => {
    if (isLoading) return;

    const saveState = async () => {
      try {
        const dataToSave = JSON.stringify({
          tasks,
          expandedSections,
          lastUpdated: new Date().toISOString()
        });
        await window.storage.set(STORAGE_KEY, dataToSave);
        setSaveStatus('✓ Saved');
        setTimeout(() => setSaveStatus(''), 2000);
      } catch (error) {
        console.error('Failed to save:', error);
        setSaveStatus('⚠ Save failed');
      }
    };

    saveState();
  }, [tasks, isLoading]);

  const toggleSection = (section) => {
    setExpandedSections(prev => ({
      ...prev,
      [section]: !prev[section]
    }));
  };

  const toggleTask = (taskId) => {
    setTasks(prev => ({
      ...prev,
      [taskId]: !prev[taskId]
    }));
  };

  const getPhaseProgress = (taskIds) => {
    const completed = taskIds.filter(id => tasks[id]).length;
    return { completed, total: taskIds.length, percent: Math.round((completed / taskIds.length) * 100) };
  };

  const resetProgress = async () => {
    if (confirm('Are you sure you want to reset all progress? This cannot be undone.')) {
      try {
        await window.storage.delete(STORAGE_KEY);
        window.location.reload();
      } catch (error) {
        console.error('Failed to reset:', error);
      }
    }
  };

  const TaskItem = ({ id, label, requirement, dataFile, notes, priority }) => (
    <div
      className={`flex items-start gap-3 p-3 rounded-lg cursor-pointer transition-all ${
        tasks[id] ? 'bg-green-50 border border-green-200' : 'bg-white border border-gray-200 hover:border-indigo-300'
      }`}
      onClick={() => toggleTask(id)}
    >
      {tasks[id] ? (
        <CheckCircle2 className="w-5 h-5 text-green-600 mt-0.5 flex-shrink-0" />
      ) : (
        <Circle className="w-5 h-5 text-gray-400 mt-0.5 flex-shrink-0" />
      )}
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2 flex-wrap">
          <span className={`font-medium ${tasks[id] ? 'text-green-900 line-through' : 'text-gray-900'}`}>
            {label}
          </span>
          {priority && (
            <span className="px-2 py-0.5 text-xs font-medium bg-orange-100 text-orange-700 rounded">
              {priority}
            </span>
          )}
        </div>
        {requirement && (
          <p className="text-sm text-gray-600 mt-1">{requirement}</p>
        )}
        {dataFile && (
          <p className="text-xs text-indigo-600 mt-1 font-mono">{dataFile}</p>
        )}
        {notes && (
          <p className="text-xs text-gray-500 mt-1 italic">{notes}</p>
        )}
      </div>
    </div>
  );

  const phases = [
    {
      id: 'phase1',
      title: 'Phase 1: Data Pipeline (Week 1)',
      icon: Database,
      color: 'blue',
      hours: '8 hrs',
      tasks: [
        {
          id: 'setup-repo-structure',
          label: 'Set Up Repository Structure',
          requirement: 'Create wbb_polls_pbp_2026/ with subdirectories',
          notes: 'data/, scripts/, docs/, tableau/',
          priority: 'HIGH'
        },
        {
          id: 'install-dependencies',
          label: 'Install Dependencies',
          requirement: 'Set up Python environment',
          notes: 'pandas, requests, beautifulsoup4, lxml'
        },
        {
          id: 'create-gitignore',
          label: 'Create .gitignore',
          requirement: 'Exclude raw CSVs, credentials, temp files'
        },
        {
          id: 'build-scraper',
          label: 'Build AP Poll Scraper Script',
          requirement: 'Scrape Sports-Reference AP Poll pages',
          dataFile: 'scripts/scrape_polls.py',
          priority: 'HIGH'
        },
        {
          id: 'store-raw-data',
          label: 'Store Raw Poll Data',
          requirement: 'Save as polls_1_YYYYMMDD.csv, polls_2_YYYYMMDD.csv',
          notes: 'One file per poll release'
        },
        {
          id: 'build-polls-long',
          label: 'Build Analytical Table Generator',
          requirement: 'Create polls_long.csv (one row per team per poll week)',
          dataFile: 'scripts/build_polls_long.py',
          priority: 'HIGH'
        },
        {
          id: 'implement-current-rank',
          label: 'Implement Current Rank Logic',
          requirement: 'Numeric rank, unranked = 26',
          dataFile: 'scripts/metrics.py'
        },
        {
          id: 'implement-previous-rank',
          label: 'Implement Previous Week Rank',
          requirement: 'Table calculation / lag logic',
          dataFile: 'scripts/metrics.py'
        },
        {
          id: 'implement-rank-change',
          label: 'Implement Rank Change (WoW)',
          requirement: 'Week-over-week rank delta',
          dataFile: 'scripts/metrics.py'
        },
        {
          id: 'implement-movement-category',
          label: 'Implement Movement Category',
          requirement: 'Big Rise (≥+5), Small Rise, Flat, Drop, Big Drop (≤-5)',
          dataFile: 'scripts/metrics.py'
        },
        {
          id: 'implement-ranked-flags',
          label: 'Implement Ranked/Unranked Flags',
          requirement: 'Boolean flags for filtering',
          dataFile: 'scripts/metrics.py'
        },
        {
          id: 'implement-conference-grouping',
          label: 'Implement Conference Grouping',
          requirement: 'Conference assignment per team',
          dataFile: 'scripts/metrics.py'
        }
      ]
    },
    {
      id: 'phase2',
      title: 'Phase 2: Tableau Build (Week 2-3)',
      icon: BarChart3,
      color: 'purple',
      hours: '12 hrs',
      tasks: [
        {
          id: 'tableau-snapshot-table',
          label: 'Snapshot Table View',
          requirement: 'Team, Conference, Current Rank, Rank Change, Movement Category',
          notes: 'Sort by current rank, color by movement category',
          priority: 'HIGH'
        },
        {
          id: 'tableau-rank-change-bar',
          label: 'Rank Change Bar Chart',
          requirement: 'Diverging bars centered on zero',
          notes: 'Positive = rank improvement, Negative = rank decline',
          priority: 'HIGH'
        },
        {
          id: 'tableau-volatility-view',
          label: 'Volatility View',
          requirement: 'Std Dev of Rank across weeks',
          notes: 'Scatter or bar ranking teams by stability',
          priority: 'HIGH'
        },
        {
          id: 'tableau-conference-lens',
          label: 'Conference Lens View',
          requirement: 'Median rank by conference over time',
          notes: 'Rank distribution per poll week'
        },
        {
          id: 'filter-poll-week',
          label: 'Poll Week Selector',
          requirement: 'Dropdown to select which poll week to display',
          notes: 'Show date in selector'
        },
        {
          id: 'filter-conference',
          label: 'Conference Filter',
          requirement: 'Filter by conference grouping',
          notes: 'Multi-select enabled'
        },
        {
          id: 'filter-movement-category',
          label: 'Movement Category Filter',
          requirement: 'Filter by Big Rise, Drop, etc.',
          notes: 'Quick highlight of movers'
        },
        {
          id: 'filter-team-search',
          label: 'Team Search/Highlight',
          requirement: 'Search or select specific teams',
          notes: 'Highlight across all views'
        },
        {
          id: 'design-movement-colors',
          label: 'Movement Color Encoding',
          requirement: 'Green for rises, red for drops, gray for flat',
          notes: 'Consistent across all views'
        },
        {
          id: 'design-diverging-bars',
          label: 'Diverging Bar Styling',
          requirement: 'Zero line centered consistently',
          notes: 'Movement bands visually distinct'
        },
        {
          id: 'design-visual-hierarchy',
          label: 'Clear Visual Hierarchy',
          requirement: 'Title > Section > Chart > Annotation',
          notes: 'Font sizes: 24pt → 18pt → 12pt → 10pt'
        },
        {
          id: 'design-mobile-friendly',
          label: 'Mobile-Friendly Layout',
          requirement: 'Test on phone viewport',
          notes: 'Consider vertical stacking'
        },
        {
          id: 'annotation-tooltips',
          label: 'Tooltip Strategy',
          requirement: 'Explain *why* movement matters, not just what moved',
          notes: 'Use relative language (conference context)',
          priority: 'HIGH'
        },
        {
          id: 'annotation-captions',
          label: 'Caption Guidelines',
          requirement: 'Keep captions under 2 lines',
          notes: 'Tableau Public readability'
        },
        {
          id: 'annotation-movement-context',
          label: 'Movement Context Annotations',
          requirement: 'Call out notable rises/drops',
          notes: 'Conference context where relevant'
        },
        {
          id: 'annotation-volatility-insights',
          label: 'Volatility Insights',
          requirement: 'Highlight most/least stable teams',
          notes: 'Narrative for portfolio value'
        }
      ]
    },
    {
      id: 'phase3',
      title: 'Phase 3: Automation & QA (Week 4)',
      icon: Zap,
      color: 'green',
      hours: '3 hrs',
      tasks: [
        {
          id: 'qa-scraper-errors',
          label: 'Scraper Error Handling',
          requirement: 'Scraper runs without errors',
          notes: 'Handle missing weeks, network failures',
          priority: 'HIGH'
        },
        {
          id: 'qa-append-logic',
          label: 'Append Logic Validation',
          requirement: 'New poll weeks append correctly',
          notes: 'No duplicate rows'
        },
        {
          id: 'qa-unranked-handling',
          label: 'Unranked Team Handling',
          requirement: 'Consistent treatment (rank = 26)',
          notes: 'Verify in all views'
        },
        {
          id: 'qa-filter-integrity',
          label: 'Filter Integrity Check',
          requirement: 'Filters do not break table calcs',
          notes: 'Test all filter combinations'
        },
        {
          id: 'qa-zero-line',
          label: 'Zero Line Verification',
          requirement: 'Zero line centered consistently in bar chart',
          notes: 'Check edge cases'
        },
        {
          id: 'qa-cross-view',
          label: 'Cross-View Consistency',
          requirement: 'Same teams, same colors, same logic across views',
          notes: 'No contradictions'
        }
      ]
    },
    {
      id: 'phase4',
      title: 'Phase 4: Publishing (Week 4)',
      icon: Target,
      color: 'red',
      hours: '2 hrs',
      tasks: [
        {
          id: 'publish-tableau-public',
          label: 'Publish to Tableau Public',
          requirement: 'Upload dashboard with public sharing enabled',
          notes: 'Test all interactions live',
          priority: 'HIGH'
        },
        {
          id: 'update-dashboard-description',
          label: 'Update Dashboard Description',
          requirement: 'Clear description for Tableau Public listing',
          notes: 'Include data source attribution'
        },
        {
          id: 'update-readme',
          label: 'Update README',
          requirement: 'Align README with dashboard narrative',
          notes: 'Include screenshots',
          priority: 'HIGH'
        },
        {
          id: 'portfolio-integration',
          label: 'Portfolio Integration',
          requirement: 'Link to portfolio site',
          notes: 'Add to project showcase'
        }
      ]
    }
  ];

  const overallProgress = getPhaseProgress(Object.keys(tasks));

  return (
    <div className="min-h-screen bg-gradient-to-br from-indigo-50 to-gray-100 p-6">
      <div className="max-w-6xl mx-auto">
        {/* Header */}
        <div className="bg-white rounded-xl shadow-lg p-6 mb-6">
          <div className="flex items-center justify-between mb-4">
            <div>
              <h1 className="text-3xl font-bold text-gray-900">{PROJECT_NAME}</h1>
              <p className="text-gray-600 mt-1">Weekly AP Poll Analytics with Rank Movement & Conference Views</p>
              <p className="text-sm text-indigo-600 mt-1">Timeline: 4 weeks • 25 hours total</p>
            </div>
            <div className="flex items-center gap-4">
              <div className="text-right">
                <div className="text-sm text-gray-600">Overall Progress</div>
                <div className="text-2xl font-bold text-indigo-600">
                  {overallProgress.percent}%
                </div>
                <div className="text-xs text-gray-500">
                  {overallProgress.completed}/{overallProgress.total} tasks
                </div>
              </div>
              <button
                onClick={resetProgress}
                className="p-2 text-gray-600 hover:text-red-600 hover:bg-red-50 rounded-lg transition-colors"
                title="Reset all progress"
              >
                <RefreshCw className="w-5 h-5" />
              </button>
            </div>
          </div>

          {/* Progress Bar */}
          <div className="w-full bg-gray-200 rounded-full h-3">
            <div
              className="bg-gradient-to-r from-indigo-500 to-indigo-600 h-3 rounded-full transition-all duration-500"
              style={{ width: `${overallProgress.percent}%` }}
            />
          </div>

          {/* Key Deliverables */}
          <div className="mt-4 grid grid-cols-4 gap-3">
            <div className="bg-blue-50 p-3 rounded-lg">
              <div className="text-xs text-blue-600 font-semibold">DATA PIPELINE</div>
              <div className="text-sm text-gray-700 mt-1">Scraper + polls_long.csv</div>
            </div>
            <div className="bg-purple-50 p-3 rounded-lg">
              <div className="text-xs text-purple-600 font-semibold">TABLEAU DASHBOARD</div>
              <div className="text-sm text-gray-700 mt-1">4 views + filters</div>
            </div>
            <div className="bg-green-50 p-3 rounded-lg">
              <div className="text-xs text-green-600 font-semibold">QA & AUTOMATION</div>
              <div className="text-sm text-gray-700 mt-1">Error handling + validation</div>
            </div>
            <div className="bg-red-50 p-3 rounded-lg">
              <div className="text-xs text-red-600 font-semibold">PORTFOLIO</div>
              <div className="text-sm text-gray-700 mt-1">Tableau Public + README</div>
            </div>
          </div>

          {/* Save Status */}
          {saveStatus && (
            <div className="mt-3 text-sm text-gray-600 flex items-center gap-2">
              <Save className="w-4 h-4" />
              {saveStatus}
            </div>
          )}
        </div>

        {/* Phase Cards */}
        <div className="space-y-4">
          {phases.map(phase => {
            const progress = getPhaseProgress(phase.tasks.map(t => t.id));
            const Icon = phase.icon;

            return (
              <div key={phase.id} className="bg-white rounded-xl shadow-lg overflow-hidden">
                {/* Phase Header */}
                <div
                  className={`bg-gradient-to-r from-${phase.color}-500 to-${phase.color}-600 text-white p-4 cursor-pointer`}
                  onClick={() => toggleSection(phase.id)}
                >
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-3">
                      {expandedSections[phase.id] ? (
                        <ChevronDown className="w-5 h-5" />
                      ) : (
                        <ChevronRight className="w-5 h-5" />
                      )}
                      <Icon className="w-6 h-6" />
                      <div>
                        <h2 className="text-xl font-bold">{phase.title}</h2>
                        <div className="text-sm opacity-90">{phase.hours}</div>
                      </div>
                    </div>
                    <div className="flex items-center gap-4">
                      <span className="text-sm opacity-90">
                        {progress.completed}/{progress.total} complete
                      </span>
                      <div className="w-24 bg-white/20 rounded-full h-2">
                        <div
                          className="bg-white h-2 rounded-full transition-all duration-500"
                          style={{ width: `${progress.percent}%` }}
                        />
                      </div>
                    </div>
                  </div>
                </div>

                {/* Phase Tasks */}
                {expandedSections[phase.id] && (
                  <div className="p-4 space-y-2">
                    {phase.tasks.map(task => (
                      <TaskItem key={task.id} {...task} />
                    ))}
                  </div>
                )}
              </div>
            );
          })}
        </div>

        {/* Footer Notes */}
        <div className="mt-6 bg-white rounded-xl shadow-lg p-6">
          <h3 className="font-bold text-gray-900 mb-3">Success Criteria</h3>
          <div className="grid grid-cols-2 gap-4 text-sm">
            <div>
              <div className="font-semibold text-gray-700 mb-2">Technical:</div>
              <ul className="space-y-1 text-gray-600">
                <li>• Scraper runs without manual intervention</li>
                <li>• New poll weeks append without duplicates</li>
                <li>• Unranked teams handled consistently (rank = 26)</li>
                <li>• All Tableau filters work correctly</li>
              </ul>
            </div>
            <div>
              <div className="font-semibold text-gray-700 mb-2">Portfolio Value:</div>
              <ul className="space-y-1 text-gray-600">
                <li>• Non-technical viewer understands rank movement in &lt;30s</li>
                <li>• Demonstrates calculated fields (not just raw ranks)</li>
                <li>• Shows conference context for every team</li>
                <li>• Dashboard tells a story, not just displays data</li>
              </ul>
            </div>
          </div>

          <h3 className="font-bold text-gray-900 mt-6 mb-3">Future Expansion Ideas</h3>
          <ul className="text-sm text-gray-600 space-y-1">
            <li>• Rank momentum index (rolling avg change)</li>
            <li>• Tournament seeding projection overlay</li>
            <li>• Conference depth score</li>
            <li>• Historical season comparisons</li>
          </ul>
        </div>
      </div>
    </div>
  );
};

export default ProjectTracker;
