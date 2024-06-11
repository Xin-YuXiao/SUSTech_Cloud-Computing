% 函数用于去除离群点和返回索引值
function [mean_value, indices] = RemoveOutliers(data)
    % 将数据转换为列向量
    data = data(:);
    n = length(data);
    k = 5; % 每次去掉5个离群点
    if k >= n
        error('k should be less than the number of elements in the array');
    end
    
    % 初始均值
    mean_value = mean(data);
    
    % 创建一个初始的索引数组
    indices = 1:n;
    
    % 从两端向中间去除离群点
    while true
        % 计算与当前均值的差距
        diff = abs(data - mean_value);
        
        % 找到最大差距的索引
        [~, outlier_index] = max(diff);
        
        % 判断是否超过移除后新均值的10%的偏差
        new_mean_value = mean(data);
        if abs((diff(outlier_index)-abs(new_mean_value))/(new_mean_value)) <= 0.5
            break;
        end
        
        % 从左侧或右侧删除k个数据点
        if outlier_index <= n / 2
            % 左侧删除k个数据点
            data(1:k) = [];
            indices(1:k) = [];
        else
            % 右侧删除k个数据点
            data(end-k+1:end) = [];
            indices(end-k+1:end) = [];
        end
        
        % 更新均值
        mean_value = mean(data);
        
        % 更新n
        n = length(data);
        
        % 检查是否剩余的数据点少于k个
        if n <= k
            break;
        end
    end
end

