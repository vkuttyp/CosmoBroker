using System.Collections.Generic;
using CosmoBroker.Models;

namespace CosmoBroker.Services;

public class SubjectMapper
{
    private readonly List<SubjectMapping> _mappings = new();

    public void AddMapping(SubjectMapping mapping)
    {
        _mappings.Add(mapping);
    }

    public string Map(string subject)
    {
        foreach (var mapping in _mappings)
        {
            var result = mapping.Map(subject);
            if (result != null) return result;
        }
        return subject;
    }
}
