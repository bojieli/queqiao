When a coded data flow stops making forward progress, subsequent retries use
the authenticated reliable stream instead of repeatedly sending unreliable
coded data. This lets server-side responses recover without a client lane
manager.
