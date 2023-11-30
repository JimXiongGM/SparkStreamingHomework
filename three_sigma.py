import numpy as np

def detect_outliers(data):
    mean = np.mean(data)
    std_dev = np.std(data)
    threshold = mean + 3 * std_dev

    print("Mean:", mean)
    print("Standard deviation:", std_dev)
    print("Threshold:", threshold)
    
    outliers = []
    for value in data:
        if np.abs(value) > threshold:
            outliers.append(value)
    
    return outliers

# 示例数据
data = np.array([1, 2, 3, 4, 5, 6, 7, 1000, 9, 10, 11, 12, 13, 14, 15])

# 检测异常值
outliers = detect_outliers(data)

if len(outliers) > 0:
    print("Detected outliers:", outliers)
else:
    print("No outliers detected.")
