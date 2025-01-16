class PurgedKFold:
    """
    Implements Purged K-Fold Cross-Validation with Embargoing.
    """
    def __init__(self, n_splits=5, purge_size=0, embargo_pct=0.0):
        """
        Parameters:
        - n_splits: int, Number of folds.
        - purge_size: int, Number of samples to purge before the test set.
        - embargo_pct: float, Proportion of samples to embargo after the test set.
        """
        self.n_splits = n_splits
        self.purge_size = purge_size
        self.embargo_pct = embargo_pct

    def split(self, X):
        """
        Splits the data into train/test indices with purging and embargoing.

        Parameters:
        - X: array-like, Dataset of shape (n_samples,).

        Returns:
        - List of (train_indices, test_indices) tuples.
        """
        n_samples = len(X)
        embargo_size = int(n_samples * self.embargo_pct)
        kf = KFold(n_splits=self.n_splits, shuffle=False)

        splits = []
        for train_indices, test_indices in kf.split(X):
            # Start and end of the test set
            test_start = test_indices[0]
            test_end = test_indices[-1] + 1

            # Purge: Remove data before the test set
            purge_start = max(0, test_start - self.purge_size)

            # Embargo: Remove data after the test set
            embargo_end = min(n_samples, test_end + embargo_size)

            # Remove purged and embargoed regions from train indices
            train_indices = train_indices[(train_indices < purge_start) | (train_indices >= embargo_end)]
            splits.append((train_indices, test_indices))

        return splits