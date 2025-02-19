/**
 * Process Details JavaScript
 * Handles process details page functionality
 */

document.addEventListener('DOMContentLoaded', function() {
    // Initialize tooltips
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });
    
    // Setup tabs
    setupTabs();
    
    // Setup refresh functionality
    setupRefresh();
    
    // Handle document downloads tracking
    trackDownloads();
});

/**
 * Setup tab functionality
 */
function setupTabs() {
    // Handle tab changes via URL hash
    const hash = window.location.hash;
    if (hash) {
        const tab = document.querySelector(`.nav-link[data-bs-target="${hash}"]`);
        if (tab) {
            const tabInstance = new bootstrap.Tab(tab);
            tabInstance.show();
        }
    }
    
    // Update URL hash when tab changes
    const tabs = document.querySelectorAll('button[data-bs-toggle="tab"]');
    tabs.forEach(tab => {
        tab.addEventListener('shown.bs.tab', function(event) {
            const targetId = event.target.getAttribute('data-bs-target');
            history.replaceState(null, null, targetId);
        });
    });
}

/**
 * Setup automatic refresh functionality
 */
function setupRefresh() {
    // Add refresh button to card headers
    const refreshButtons = document.querySelectorAll('.refresh-btn');
    refreshButtons.forEach(button => {
        button.addEventListener('click', function(event) {
            event.preventDefault();
            const tabId = this.closest('.tab-pane').id;
            refreshTabContent(tabId);
        });
    });
    
    // Auto-refresh for active processes
    const processStatus = document.querySelector('.process-status').textContent.trim();
    if (['PROCESSING', 'PAUSED', 'MANUAL_REVIEW'].includes(processStatus)) {
        // Refresh every 30 seconds for active processes
        setInterval(function() {
            const activeTab = document.querySelector('.tab-pane.active').id;
            refreshTabContent(activeTab);
        }, 30000);
    }
}

/**
 * Refresh tab content via AJAX
 * @param {string} tabId - The ID of the tab to refresh
 */
function refreshTabContent(tabId) {
    const processId = getProcessId();
    let endpoint = '';
    
    // Show loading spinner
    const tabPane = document.getElementById(tabId);
    const loadingSpinner = document.createElement('div');
    loadingSpinner.className = 'text-center py-3 loading-spinner';
    loadingSpinner.innerHTML = '<i class="fas fa-spinner fa-spin fa-2x"></i>';
    
    // Determine endpoint based on tab
    switch(tabId) {
        case 'details':
            endpoint = `/api/process/${processId}`;
            break;
        case 'documents':
            endpoint = `/api/process/${processId}/documents`;
            break;
        case 'outputs':
            endpoint = `/api/process/${processId}/outputs`;
            break;
        case 'timeline':
            endpoint = `/api/process/${processId}/timeline`;
            break;
        default:
            return; // Unknown tab, exit
    }
    
    // Add loading spinner
    const contentContainer = tabPane.querySelector('.tab-content-container') || tabPane;
    contentContainer.style.opacity = '0.5';
    tabPane.appendChild(loadingSpinner);
    
    // Fetch updated data
    fetch(endpoint)
        .then(response => {
            if (!response.ok) {
                throw new Error('Network response was not ok');
            }
            return response.json();
        })
        .then(data => {
            updateTabContent(tabId, data);
            
            // Remove loading spinner
            contentContainer.style.opacity = '1';
            tabPane.removeChild(loadingSpinner);
            
            // Show success notification
            showNotification('Data refreshed successfully', 'success');
        })
        .catch(error => {
            console.error('Error refreshing data:', error);
            
            // Remove loading spinner
            contentContainer.style.opacity = '1';
            if (tabPane.contains(loadingSpinner)) {
                tabPane.removeChild(loadingSpinner);
            }
            
            // Show error notification
            showNotification('Failed to refresh data', 'danger');
        });
}

/**
 * Update tab content with new data
 * @param {string} tabId - The ID of the tab to update
 * @param {Object} data - The new data for the tab
 */
function updateTabContent(tabId, data) {
    switch(tabId) {
        case 'details':
            updateDetailsTab(data);
            break;
        case 'documents':
            updateDocumentsTab(data);
            break;
        case 'outputs':
            updateOutputsTab(data);
            break;
        case 'timeline':
            updateTimelineTab(data);
            break;
    }
}

/**
 * Update details tab with new data
 * @param {Object} data - The process details data
 */
function updateDetailsTab(data) {
    // Update process status
    const statusBadge = document.querySelector('.process-status');
    if (statusBadge) {
        statusBadge.className = getStatusBadgeClass(data.status);
        statusBadge.textContent = data.status;
    }
    
    // Update current stage
    const currentStage = document.querySelector('.current-stage');
    if (currentStage) {
        currentStage.textContent = data.current_stage || 'N/A';
    }
    
    // Update last updated time
    const lastUpdated = document.querySelector('.last-updated');
    if (lastUpdated) {
        lastUpdated.textContent = formatDateTime(data.last_updated);
    }
    
    // Update action buttons visibility based on status
    updateActionButtons(data.status);
}

/**
 * Update documents tab with new data
 * @param {Array} documents - The documents data
 */
function updateDocumentsTab(documents) {
    const table = $('#documentsTable').DataTable();
    
    // Clear existing data
    table.clear();
    
    // Add new data
    documents.forEach(doc => {
        const fileTypeIcon = getFileTypeIcon(doc.file_type);
        const uploadedAt = formatDateTime(doc.uploaded_at);
        
        table.row.add([
            doc.filename,
            `${fileTypeIcon} ${capitalizeFirstLetter(doc.file_type)}`,
            doc.file_size_readable,
            uploadedAt,
            `<a href="/download/${doc.file_id}" class="btn btn-sm btn-outline-primary">
                <i class="fas fa-download me-1"></i> Download
            </a>`
        ]);
    });
    
    // Redraw the table
    table.draw();
}

/**
 * Update outputs tab with new data
 * @param {Array} outputs - The outputs data
 */
function updateOutputsTab(outputs) {
    const table = $('#outputsTable').DataTable();
    
    // Clear existing data
    table.clear();
    
    // Add new data
    outputs.forEach(output => {
        const fileTypeIcon = getFileTypeIcon(output.file_type);
        const generatedAt = formatDateTime(output.generated_at);
        
        table.row.add([
            output.filename,
            `${fileTypeIcon} ${capitalizeFirstLetter(output.file_type)}`,
            output.file_size_readable,
            generatedAt,
            `<a href="/download/${output.file_id}" class="btn btn-sm btn-outline-primary">
                <i class="fas fa-download me-1"></i> Download
            </a>`
        ]);
    });
    
    // Redraw the table
    table.draw();
}

/**
 * Update timeline tab with new data
 * @param {Array} events - The timeline events
 */
function updateTimelineTab(events) {
    const timelineContainer = document.querySelector('.timeline-container');
    if (!timelineContainer) return;
    
    // Sort events by timestamp
    events.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
    
    let html = '';
    
    if (events.length === 0) {
        html = `
            <div class="text-center py-4">
                <p class="text-muted">No timeline events available</p>
            </div>
        `;
    } else {
        events.forEach(event => {
            const markerClass = getEventMarkerClass(event.event_type);
            const icon = getEventIcon(event.event_type);
            const title = getEventTitle(event.event_type);
            const timestamp = formatDateTime(event.timestamp);
            const content = getEventContent(event);
            
            html += `
                <div class="timeline-item">
                    <div class="timeline-marker ${markerClass}">
                        <i class="${icon}"></i>
                    </div>
                    <div class="timeline-content">
                        <div class="timeline-header">
                            <span class="timeline-title">${title}</span>
                            <span class="timeline-date">${timestamp}</span>
                        </div>
                        <div class="timeline-body">
                            ${content}
                        </div>
                    </div>
                </div>
            `;
        });
    }
    
    timelineContainer.innerHTML = html;
}

/**
 * Update action buttons based on process status
 * @param {string} status - The process status
 */
function updateActionButtons(status) {
    const resumeButton = document.querySelector('.btn-resume-process');
    const cancelButton = document.querySelector('.btn-cancel-process');
    
    if (resumeButton) {
        if (['PAUSED', 'ERROR', 'MANUAL_REVIEW'].includes(status)) {
            resumeButton.style.display = 'inline-block';
        } else {
            resumeButton.style.display = 'none';
        }
    }
    
    if (cancelButton) {
        if (!['COMPLETED', 'CANCELLED', 'FAILED'].includes(status)) {
            cancelButton.style.display = 'inline-block';
        } else {
            cancelButton.style.display = 'none';
        }
    }
}

/**
 * Get file type icon HTML
 * @param {string} fileType - The file type
 * @returns {string} HTML for the file type icon
 */
function getFileTypeIcon(fileType) {
    switch(fileType) {
        case 'excel':
            return '<i class="fas fa-file-excel text-success me-1"></i>';
        case 'image':
            return '<i class="fas fa-file-image text-primary me-1"></i>';
        case 'pdf':
            return '<i class="fas fa-file-pdf text-danger me-1"></i>';
        default:
            return '<i class="fas fa-file text-secondary me-1"></i>';
    }
}

/**
 * Get status badge CSS class
 * @param {string} status - The process status
 * @returns {string} CSS class for the status badge
 */
function getStatusBadgeClass(status) {
    let baseClass = 'badge rounded-pill p-2 process-status';
    
    switch(status) {
        case 'COMPLETED':
            return `${baseClass} bg-success`;
        case 'PROCESSING':
            return `${baseClass} bg-primary`;
        case 'ERROR':
            return `${baseClass} bg-danger`;
        case 'MANUAL_REVIEW':
            return `${baseClass} bg-warning`;
        case 'PAUSED':
            return `${baseClass} bg-secondary`;
        case 'CANCELLED':
            return `${baseClass} bg-dark`;
        default:
            return `${baseClass} bg-info`;
    }
}

/**
 * Get timeline event marker CSS class
 * @param {string} eventType - The event type
 * @returns {string} CSS class for the event marker
 */
function getEventMarkerClass(eventType) {
    switch(eventType) {
        case 'process_created':
            return 'bg-primary';
        case 'process_completed':
            return 'bg-success';
        case 'process_error':
            return 'bg-danger';
        case 'process_resumed':
            return 'bg-info';
        case 'process_cancelled':
            return 'bg-dark';
        default:
            return 'bg-secondary';
    }
}

/**
 * Get timeline event icon class
 * @param {string} eventType - The event type
 * @returns {string} CSS class for the event icon
 */
function getEventIcon(eventType) {
    switch(eventType) {
        case 'process_created':
            return 'fas fa-play';
        case 'process_completed':
            return 'fas fa-check';
        case 'process_error':
            return 'fas fa-exclamation';
        case 'process_resumed':
            return 'fas fa-redo';
        case 'process_cancelled':
            return 'fas fa-times';
        case 'status_change':
            return 'fas fa-exchange-alt';
        case 'stage_change':
            return 'fas fa-arrow-right';
        case 'note_added':
            return 'fas fa-sticky-note';
        default:
            return 'fas fa-circle';
    }
}

/**
 * Get timeline event title
 * @param {string} eventType - The event type
 * @returns {string} Title for the event
 */
function getEventTitle(eventType) {
    switch(eventType) {
        case 'process_created':
            return 'Process Created';
        case 'process_completed':
            return 'Process Completed';
        case 'process_error':
            return 'Error Occurred';
        case 'process_resumed':
            return 'Process Resumed';
        case 'process_cancelled':
            return 'Process Cancelled';
        case 'status_change':
            return 'Status Changed';
        case 'stage_change':
            return 'Stage Changed';
        case 'note_added':
            return 'Note Added';
        default:
            return capitalizeFirstLetter(eventType.replace('_', ' '));
    }
}

/**
 * Get timeline event content HTML
 * @param {Object} event - The event object
 * @returns {string} HTML content for the event
 */
function getEventContent(event) {
    if (!event.details) return '';
    
    switch(event.event_type) {
        case 'status_change':
            return `Status changed from <strong>${event.details.previous_status}</strong> to <strong>${event.details.new_status}</strong>`;
            
        case 'stage_change':
            return `Processing moved from <strong>${event.details.previous_stage}</strong> to <strong>${event.details.new_stage}</strong>`;
            
        case 'process_error':
            return `
                <div class="alert alert-danger">
                    ${event.details.error_message}
                </div>
                <small>Error occurred in stage: <strong>${event.details.stage}</strong></small>
            `;
            
        case 'note_added':
            return `
                <div class="card">
                    <div class="card-body bg-light py-2">
                        ${event.details.note}
                    </div>
                </div>
                <small>Added by: ${event.user_id}</small>
            `;
            
        case 'process_resumed':
            let resumeContent = `<p>Process resumed from status: <strong>${event.details.previous_status}</strong></p>`;
            if (event.details.notes) {
                resumeContent += `<small>Note: ${event.details.notes}</small>`;
            }
            return resumeContent;
            
        case 'process_cancelled':
            return `
                <p>Process cancelled from status: <strong>${event.details.previous_status}</strong></p>
                <small>Reason: ${event.details.reason}</small>
            `;
            
        default:
            return `<pre class="timeline-details">${JSON.stringify(event.details, null, 2)}</pre>`;
    }
}

/**
 * Track document downloads
 */
function trackDownloads() {
    const downloadButtons = document.querySelectorAll('a[href^="/download/"]');
    downloadButtons.forEach(button => {
        button.addEventListener('click', function(event) {
            const fileId = this.getAttribute('href').replace('/download/', '');
            // Log download via analytics API
            fetch('/api/log-download', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    file_id: fileId,
                    process_id: getProcessId()
                })
            }).catch(error => {
                console.error('Error logging download:', error);
            });
        });
    });
}

/**
 * Helper function to get process ID from the URL
 * @returns {string} The process ID
 */
function getProcessId() {
    const pathParts = window.location.pathname.split('/');
    return pathParts[pathParts.length - 1];
}

/**
 * Format date-time string
 * @param {string} dateTimeStr - ISO date-time string
 * @returns {string} Formatted date-time string
 */
function formatDateTime(dateTimeStr) {
    if (!dateTimeStr) return 'N/A';
    return dateTimeStr.replace('T', ' ').substring(0, 19);
}

/**
 * Capitalize first letter of a string
 * @param {string} str - Input string
 * @returns {string} String with first letter capitalized
 */
function capitalizeFirstLetter(str) {
    if (!str) return '';
    return str.charAt(0).toUpperCase() + str.slice(1);
}

/**
 * Show a notification message
 * @param {string} message - Message to display
 * @param {string} type - Message type (success, danger, warning, info)
 */
function showNotification(message, type = 'info') {
    // Create notification element
    const notification = document.createElement('div');
    notification.className = `alert alert-${type} alert-dismissible fade show notification-toast`;
    notification.setAttribute('role', 'alert');
    notification.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
    `;
    
    // Add to notification container (create if it doesn't exist)
    let notificationContainer = document.querySelector('.notification-container');
    if (!notificationContainer) {
        notificationContainer = document.createElement('div');
        notificationContainer.className = 'notification-container position-fixed top-0 end-0 p-3';
        notificationContainer.style.zIndex = '1050';
        document.body.appendChild(notificationContainer);
    }
    
    // Add notification to container
    notificationContainer.appendChild(notification);
    
    // Create Bootstrap alert instance
    const alert = new bootstrap.Alert(notification);
    
    // Auto-hide after 5 seconds
    setTimeout(() => {
        alert.close();
    }, 5000);
}