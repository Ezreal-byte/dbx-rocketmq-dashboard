// Sandboxed DBX frames have no browser storage origin. Keep presentation
// preferences in the current workbench; connection state belongs to DBX.
const preferences = new Map();
export const presentationPreferences = {
    getItem: key => preferences.get(key) ?? null,
    setItem: (key,value) => preferences.set(key,value),
};
