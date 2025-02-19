/**
 * Dashboard JavaScript
 * Handles dashboard-specific functionality
 */

document.addEventListener('DOMContentLoaded', function() {
    // Initialize tooltips
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });
    
    // Handle process actions
    setupProcessActions();
    
    // Refresh dashboard data periodically (every 60 seconds)
    setInterval(refreshDashboardData, 60000);
});

/**
 * Setup process action buttons
 */
function setupProcessActions() {
    // Resume process button handling
    $('.resume-process').on('click', function() {
        const processId = $(this).data('process-id');
        const processSubject = $(this).closest('tr').find('td:eq(2)').text();
        
        $('#resumeProcessModalLabel').text(`Resume Process: ${processId}`);
        $('#resumeProcessForm').attr('action', `/process/${processId}/resume`);
        $('#resumeProcessModal .modal-body p').html(`
            Are you sure you want to resume processing for:<br>
            <strong>${processSubject}</strong>?
        `);
    });
    
    // Cancel process button handling
    $('.cancel-process').on('click', function() {
        const processId = $(this).data('process-id');
        const processSubject = $(this).closest('tr').find('td:eq(2)').text();
        
        $('#cancelProcessModalLabel').text(`Cancel Process: ${processId}`);
        $('#cancelProcessForm').attr('action', `/process/${processId}/cancel`);
        $('#cancelProcessModal .modal-body .alert').html(`
            <i class="fas fa-exclamation-triangle me-2"></i>
            <strong>Warning:</strong> You are about to cancel:<br>
            <strong>${processSubject}</strong><br>
            This action cannot be undone.
        `);
    });
}

/**
 * Refresh dashboard data via AJAX
 */
function refreshDashboardData() {
    // Get current filter values
    const days = $('#daysFilter').val();
    const status = $('#statusFilter').val();
    const search = $('#searchFilter').val();
    
    // Show loading indicator
    const loadingIndicator = $('<div class="text-center py-3"><i class="fas fa-spinner fa-spin fa-2x"></i></div>');
    $('#processesTable').hide().after(loadingIndicator);
    
    // Fetch updated data
    $.ajax({
        url: '/api/processes',
        method: 'GET',
        data: {
            days: days,
            status: status,
            search: search,
            page: 1,
            per_page: 20
        },
        success: function(response) {
            updateProcessesTable(response.processes);
            updateDashboardStats();
            
            // Remove loading indicator and show table
            loadingIndicator.remove();
            $('#processesTable').show();
        },
        error: function(xhr, status, error) {
            console.error('Error refreshing dashboard data:', error);
            
            // Show error notification
            showNotification('Error refreshing data. Please try again.', 'danger');
            
            // Remove loading indicator and show table
            loadingIndicator.remove();
            $('#processesTable').show();
        }
    });
}

/**
 * Update processes table with new data
 * @param {Array} processes - List of process objects
 */
function updateProcessesTable(processes) {
    const table = $('#processesTable').DataTable();
    
    // Clear existing data
    table.clear();
    
    // Add new data
    processes.forEach(function(process) {
        const statusBadge = getStatusBadgeHtml(process.status);
        const createdAt = process.created_at.replace('T', ' ').substring(0, 16);
        
        const actions = getActionButtonsHtml(process);
        
        table.row.add([
            `<a href="/process/${process.process_id}">${process.process_id.substring(0, 8)}...</a>`,
            process.client_name,
            process.email_subject,
            statusBadge,
            createdAt,
            actions
        ]);
    });
    
    // Redraw the table
    table.draw();
    
    // Re-setup action buttons
    setupProcessActions();
}

/**
 * Generate HTML for status badge
 * @param {string} status - Process status
 * @returns {string} HTML for status badge
 */
function getStatusBadgeHtml(status) {
    let badgeClass = 'bg-info';
    
    switch(status) {
        case 'COMPLETED':
            badgeClass = 'bg-success';
            break;
        case 'PROCESSING':
            badgeClass = 'bg-primary';
            break;
        case 'ERROR':
            badgeClass = 'bg-danger';
            break;
        case 'MANUAL_REVIEW':
            badgeClass = 'bg-warning';
            break;
        case 'PAUSED':
            badgeClass = 'bg-secondary';
            break;
        case 'CANCELLED':
            badgeClass = 'bg-dark';
            break;
    }
    
    return `<span class="badge rounded-pill ${badgeClass}">${status}</span>`;
}

/**
 * Generate HTML for action buttons
 * @param {Object} process - Process object
 * @returns {string} HTML for action buttons
 */
function getActionButtonsHtml(process) {
    let html = `
        <div class="btn-group">
            <a href="/process/${process.process_id}" class="btn btn-sm btn-outline-primary">
                <i class="fas fa-eye"></i>
            </a>
    `;
    
    // Add resume button for appropriate statuses
    if (['PAUSED', 'ERROR', 'MANUAL_REVIEW'].includes(process.status)) {
        html += `
            <button type="button" class="btn btn-sm btn-outline-success resume-process" 
                    data-process-id="${process.process_id}" data-bs-toggle="modal" 
                    data-bs-target="#resumeProcessModal">
                <i class="fas fa-play"></i>
            </button>
        `;
    }
    
    // Add cancel button for non-terminal statuses
    if (!['COMPLETED', 'CANCELLED', 'FAILED'].includes(process.status)) {
        html += `
            <button type="button" class="btn btn-sm btn-outline-danger cancel-process" 
                    data-process-id="${process.process_id}" data-bs-toggle="modal" 
                    data-bs-target="#cancelProcessModal">
                <i class="fas fa-times"></i>
            </button>
        `;
    }
    
    html += '</div>';
    return html;
}

/**
 * Update dashboard statistics via AJAX
 */
function updateDashboardStats() {
    $.ajax({
        url: '/api/stats',
        method: 'GET',
        data: {
            days: $('#daysFilter').val()
        },
        success: function(stats) {
            // Update statistics cards
            $('.stat-active-processes').text(stats.active_processes);
            $('.stat-success-rate').text(stats.success_rate + '%');
            $('.stat-completed-24h').text(stats.completed_last_24h);
            $('.stat-attention-needed').text(stats.attention_needed);
            
            // If attention needed count changed, refresh that section
            if (stats.attention_needed > 0 && $('.attention-needed-list').length === 0) {
                refreshAttentionNeededSection();
            }
        },
        error: function(xhr, status, error) {
            console.error('Error updating dashboard stats:', error);
        }
    });
}

/**
 * Show a notification message
 * @param {string} message - Message to display
 * @param {string} type - Message type (success, danger, warning, info)
 */
function showNotification(message, type = 'info') {
    // Create notification element
    const notification = $(`
        <div class="alert alert-${type} alert-dismissible fade show notification-toast" role="alert">
            ${message}
            <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
        </div>
    `);
    
    // Add to notification container (create if it doesn't exist)
    let notificationContainer = $('.notification-container');
    if (notificationContainer.length === 0) {
        notificationContainer = $('<div class="notification-container position-fixed top-0 end-0 p-3" style="z-index: 1050;"></div>');
        $('body').append(notificationContainer);
    }
    
    // Add notification to container
    notificationContainer.append(notification);
    
    // Auto-hide after 5 seconds
    setTimeout(function() {
        notification.alert('close');
    }, 5000);
}

/**
 * Refresh processes needing attention section
 */
function refreshAttentionNeededSection() {
    $.ajax({
        url: '/api/processes/needing-attention',
        method: 'GET',
        success: function(processes) {
            const container = $('.card-body:has(.attention-needed-list)');
            
            if (processes.length === 0) {
                // No processes need attention
                container.html(`
                    <div class="text-center py-4">
                        <i class="fas fa-check-circle fa-3x text-success mb-3"></i>
                        <p class="mb-0">No processes currently need attention</p>
                    </div>
                `);
            } else {
                // Build list of processes needing attention
                let html = '<div class="list-group attention-needed-list">';
                
                processes.forEach(function(process) {
                    const updated = process.last_updated.replace('T', ' ').substring(0, 16);
                    let badgeClass = process.status === 'ERROR' ? 'bg-danger' : 'bg-warning';
                    
                    html += `
                        <a href="/process/${process.process_id}" class="list-group-item list-group-item-action">
                            <div class="d-flex w-100 justify-content-between">
                                <h6 class="mb-1">${process.client_name}</h6>
                                <small>${updated}</small>
                            </div>
                            <p class="mb-1 text-truncate">${process.email_subject}</p>
                            <small>
                                <span class="badge rounded-pill ${badgeClass}">
                                    ${process.status}
                                </span>
                                ${process.attention_reason}
                            </small>
                        </a>
                    `;
                });
                
                html += '</div>';
                container.html(html);
            }
        }
    });
}