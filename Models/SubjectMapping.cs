using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace CosmoBroker.Models;

public class MapDestination
{
    public string Subject { get; set; } = string.Empty;
    public double Weight { get; set; } = 1.0; // 0.0 to 1.0
}

public class SubjectMapping
{
    public string SourcePattern { get; set; } = string.Empty;
    public List<MapDestination> Destinations { get; } = new();
    
    private readonly Random _random = new();

    public string? Map(string subject)
    {
        if (Destinations.Count == 0) return null;

        // Simple match or Wildcard match
        if (IsMatch(subject, out var tokens))
        {
            // Weighted selection
            double totalWeight = Destinations.Sum(d => d.Weight);
            double roll = _random.NextDouble() * totalWeight;
            
            double current = 0;
            foreach (var dest in Destinations)
            {
                current += dest.Weight;
                if (roll <= current)
                {
                    return Transform(dest.Subject, tokens);
                }
            }
        }

        return null;
    }

    private bool IsMatch(string subject, out string[] tokens)
    {
        tokens = Array.Empty<string>();
        var patternParts = SourcePattern.Split('.');
        var subjectParts = subject.Split('.');

        if (patternParts.Length != subjectParts.Length && !SourcePattern.Contains(">")) return false;

        var extracted = new List<string>();
        for (int i = 0; i < patternParts.Length; i++)
        {
            if (patternParts[i] == ">")
            {
                extracted.Add(string.Join(".", subjectParts.Skip(i)));
                tokens = extracted.ToArray();
                return true;
            }
            if (i >= subjectParts.Length) return false;
            
            if (patternParts[i] == "*")
            {
                extracted.Add(subjectParts[i]);
            }
            else if (patternParts[i] != subjectParts[i])
            {
                return false;
            }
        }

        tokens = extracted.ToArray();
        return true;
    }

    private string Transform(string template, string[] tokens)
    {
        // Replace $1, $2, etc with extracted tokens
        string result = template;
        for (int i = 0; i < tokens.Length; i++)
        {
            result = result.Replace($"${i + 1}", tokens[i]);
        }
        return result;
    }
}
